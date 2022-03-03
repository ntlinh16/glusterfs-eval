import traceback
import random
import math

from cloudal.action import performing_actions
from cloudal.configurator import packages_configurator, filebench_configurator, glusterfs_configurator, CancelException
from cloudal.experimenter import create_paramsweeper, define_parameters, get_results, is_node_active, delete_ovh_nodes
from cloudal.utils import get_logger, execute_cmd, parse_config_file, ExecuteCommandException
from cloudal.provisioner import ovh_provisioner

from execo_engine import slugify

logger = get_logger()

class glusterfs_eval_ovh(performing_actions):
    def __init__(self):
        super(glusterfs_eval_ovh, self).__init__()
        self.args_parser.add_argument("--node_ids_file", dest="node_ids_file",
                                      help="the path to the file contents list of node IDs",
                                      default=None,
                                      type=str)
        self.args_parser.add_argument("--attach_volume", dest="attach_volume",
                                      help="attach an external volume to every data node",
                                      action="store_true")
        self.args_parser.add_argument("--no_config_host", dest="no_config_host",
                                      help="do not run the functions to config the hosts",
                                      action="store_true")
        self.args_parser.add_argument("-k", dest="keep_nodes",
                                      help="keep the provisioned nodes after finishing the experiments",
                                      action="store_true")

    def save_results(self, comb, hosts):
        logger.info("----------------------------------")
        logger.info("4. Starting dowloading the results")
        get_results(comb=comb,
                    hosts=hosts,
                    remote_result_files=['/tmp/results/*'],
                    local_result_dir=self.configs['exp_env']['results_dir'])

    def run_benchmark(self, comb, hosts, gluster_mountpoint):
        benchmark = comb['benchmarks']
        logger.info('--------------------------------------')
        logger.info("3. Starting benchmark: %s" % benchmark)
        gluster_mountpoint = 'mnt\/glusterd-$(hostname)'
        filebench_hosts = random.sample(hosts, comb['n_clients'])
        configurator = filebench_configurator()
        if benchmark == 'mailserver':
            is_finished = configurator.run_mailserver(filebench_hosts, gluster_mountpoint, comb['duration'], comb['n_threads'])
            return is_finished, filebench_hosts
    
    def reset_latency(self):
        """Delete the Limitation of latency in host"""
        logger.info("--Remove network latency on hosts")
        cmd = "tcdel ens3 --all"
        execute_cmd(cmd, self.hosts)

    def get_latency(self, hostA, hostB):
        cmd = "ping -c 4 %s" % hostB
        _, r = execute_cmd(cmd, hostA)
        tokens = r.processes[0].stdout.strip().split('\r\n')[3].split('time=')
        if len(tokens) == 2:
            return  math.ceil(float(tokens[1].split()[0]))
        raise CancelException("Cannot get latency between nodes")
    
    def set_latency(self, latency):
        """Limit the latency in host"""
        logger.info('---------------------------------------')
        logger.info('Setting network latency = %s on hosts' % latency)
        clusters = dict()
        for node in self.nodes:
            cluster = node['name'].split('-')[1]
            ip = node['ipAddresses'][0]['ip']
            clusters[cluster] = clusters.get(cluster, list()) + [ip]
        logger.info('clusters = %s' % clusters)
        
        
        for cur_cluster, cur_ips in clusters.items():
            other_clusters = {cluster: hosts for cluster, hosts in clusters.items() if cluster != cur_cluster}

            for other_cluster, other_ips in other_clusters.items():
                real_latency = self.get_latency(cur_ips[0], other_ips[0])
                logger.info('latency between %s and %s is: %s' % (cur_cluster, other_cluster, real_latency))
                if real_latency < latency:
                    latency_add = (latency - real_latency)/2
                    logger.info('Add %s to current latency from %s cluster to %s:' % (latency_add, cur_cluster, other_cluster))
                    for ip in other_ips:
                        cmd = "tcset ens3 --delay %s --network %s" % (latency_add, ip)
                        
                        logger.info('%s --->  %s, cmd = %s' % (cur_ips, ip, cmd))
                        execute_cmd(cmd, cur_ips)
                else:
                    self.reset_latency()
                    return False
        return True

    def deploy_glusterfs(self, indices, gluster_mountpoint, gluster_volume_name):
        logger.info('--------------------------------------')
        logger.info("2. Deploying GlusterFS")
        configurator = glusterfs_configurator()
        return configurator.deploy_glusterfs(self.hosts, indices, gluster_mountpoint, gluster_volume_name)


    def clean_exp_env(self, hosts, gluster_mountpoint, gluster_volume_name):
        logger.info('1. Cleaning experiment environment ')
        if len(hosts) > 0:
            logger.info('Delete all files in /tmp/results folder on %s glusterfs nodes' % len(hosts))
            cmd = 'rm -rf /tmp/results && mkdir -p /tmp/results'
            execute_cmd(cmd, hosts)
            
            logger.info('Delete all data on %s glusterfs nodes' % len(hosts))
            for host in hosts:
                cmd = "mount | grep %s" % gluster_mountpoint
                _, r = execute_cmd(cmd, host)
                is_mount = r.processes[0].stdout.strip()

                if is_mount:
                    execute_cmd(cmd, host)
                    cmd = '''umount %s &&
                            rm -rf %s/* ''' % (gluster_mountpoint, gluster_mountpoint)
                    execute_cmd(cmd, host)

            logger.info('Delete all volumes on %s glusterfs nodes' % len(hosts))
            cmd = 'gluster volume list'
            _,r = execute_cmd(cmd, hosts[0])
            if gluster_volume_name in r.processes[0].stdout.strip():
                cmd = '''gluster --mode=script volume stop %s &&
                        gluster --mode=script volume delete %s''' % (gluster_volume_name, gluster_volume_name)
                execute_cmd(cmd, hosts[0])
            cmd = 'rm -rf /tmp/glusterd/*'
            execute_cmd(cmd, hosts)

    def run_exp_workflow(self, comb, sweeper, gluster_mountpoint, gluster_volume_name):
        comb_ok = 'cancel'
        try:
            logger.info('=======================================')
            logger.info('Performing combination: ' + slugify(comb))

            self.clean_exp_env(self.data_hosts, gluster_mountpoint, gluster_volume_name)
            # Get indices to select the number of GlusterFS nodes from list of hosts
            indices = random.sample(range(len(self.data_hosts)), comb['n_nodes_per_dc'] * comb['n_dc'])
            glusterfs, hosts = self.deploy_glusterfs(indices, gluster_mountpoint, gluster_volume_name)
            if glusterfs:
                if len(self.configs['exp_env']['clusters']) > 1 and comb['latency'] != 0:
                    is_latency = self.set_latency(comb["latency"])
                    if not is_latency:
                        comb_ok = 'skip'
                        sweeper.skip(comb)
                        return sweeper  
                is_finished, result_hosts = self.run_benchmark(comb, hosts, gluster_mountpoint)
                if len(self.configs['exp_env']['clusters']) > 1 and comb['latency'] != 0:
                    self.reset_latency()
                if is_finished:
                    self.save_results(comb, result_hosts)
                    comb_ok = 'done'
            else:
                raise CancelException("Cannot deploy glusterfs")
        except (ExecuteCommandException, CancelException) as e:
            logger.error('Combination exception: %s' % e)
            comb_ok = 'cancel'
        finally:
            if comb_ok == 'done':
                sweeper.done(comb)
                logger.info('Finish combination: %s' % slugify(comb))
            elif comb_ok == 'cancel':
                sweeper.cancel(comb)
                logger.warning(slugify(comb) + ' is canceled')
            else:
                sweeper.skip(comb)
                logger.warning(slugify(comb) + ' is skipped due to real_latency is higher than %s' % comb['latency'])
            logger.info('%s combinations remaining\n' % len(sweeper.get_remaining()))
        return sweeper

    def config_host(self):
        logger.info("Starting configuring nodes")
        logger.info("Installing tcconfig")
        configurator = packages_configurator()
        configurator.install_packages(['python3-pip'], self.hosts)  
        cmd = "pip3 install tcconfig"
        execute_cmd(cmd, self.hosts)

        configurator = filebench_configurator()
        configurator.install_filebench(self.hosts)
        
        configurator = glusterfs_configurator()
        configurator.install_glusterfs(self.hosts)
        logger.info("Finish configuring nodes")
    
    def calculate_latency_range(self):
        latency_interval = self.configs["parameters"]["latency_interval"]
        if latency_interval == "logarithmic scale":
            start, end = self.configs["parameters"]["latency"]
            latency = [start, end]
            log_start = int(math.ceil(math.log(start)))
            log_end = int(math.ceil(math.log(end)))
            for i in range(log_start, log_end):
                val = int(math.exp(i))
                if val < end:
                    latency.append(int(math.exp(i)))
                val = int(math.exp(i + 0.5))
                if val < end:
                    latency.append(int(math.exp(i + 0.5)))
        elif isinstance(latency_interval, int):
            start, end = self.configs["parameters"]["latency"]
            latency = [start]
            next_latency = start + latency_interval
            while next_latency < end:
                latency.append(next_latency)
                next_latency += latency_interval
            latency.append(end)
        else:
            latency = self.configs["parameters"]["latency"]
        del self.configs["parameters"]["latency_interval"]
        self.configs["parameters"]["latency"] = list(set(latency))
        logger.info('latency = %s' % self.configs["parameters"]["latency"])

    def setup_env(self):
        logger.info("STARTING SETTING THE EXPERIMENT ENVIRONMENT")
        logger.info("Starting provisioning nodes on OVHCloud")

        provisioner = ovh_provisioner(configs=self.configs, node_ids_file=self.args.node_ids_file)
        provisioner.provisioning()

        self.nodes = provisioner.nodes
        self.hosts = provisioner.hosts
        node_ids = provisioner.node_ids
        driver = provisioner.driver

        self.data_nodes = list()
        self.clusters = dict()
        for node in self.nodes:
            cluster = node['region']
            self.clusters[cluster] = [node] + self.clusters.get(cluster, list())
        for _, nodes in self.clusters.items():
            self.data_nodes += nodes[0: max(self.normalized_parameters['n_nodes_per_dc'])]
        self.data_hosts = [node['ipAddresses'][0]['ip'] for node in self.data_nodes]

        cmd = 'mkdir -p /tmp/glusterd'
        execute_cmd(cmd, self.data_hosts)

        if self.args.attach_volume:
            logger.info('Attaching external volumes to %s nodes' % len(self.data_nodes))
            provisioner.attach_volume(nodes=self.data_nodes)

            logger.info('Formatting the new external volumes')
            cmd = '''disk=$(ls -lt /dev/ | grep '^b' | head -n 1 | awk {'print $NF'})
                   mkfs.ext4 -F /dev/$disk;
                   mount -t ext4 /dev/$disk /tmp/glusterd;
                   chmod 777 /tmp/glusterd'''
            execute_cmd(cmd, self.data_hosts)

        if not self.args.no_config_host:
            self.config_host()

        self.args.node_ids_file = None

        logger.info("FINISH SETTING THE EXPERIMENT ENVIRONMENT\n")
        return node_ids, driver


    def create_combination_queue(self):
        logger.debug('Parse and convert configs for OVH provisioner')
        self.configs = parse_config_file(self.args.config_file_path)
        # Add the number of GlusterFS DC as a parameter
        self.configs['parameters']['n_dc'] = len(self.configs['exp_env']['clusters'])

        logger.debug('Normalize the parameter space')
        self.normalized_parameters = define_parameters(self.configs['parameters'])

        logger.debug('Normalize the given configs')
        n_nodes_per_cluster = max(self.normalized_parameters['n_nodes_per_dc'])
        # create standard cluster information to make reservation on OVHCloud, this info using by OVH provisioner
        clusters = list()
        for cluster in self.configs['exp_env']['clusters']:
            clusters.append({'region': cluster,
                            'n_nodes': n_nodes_per_cluster,
                            'instance_type': self.configs['instance_type'],
                            'flexible_instance': self.configs['flexible_instance'],
                            'image': self.configs['image']})
        self.configs['clusters'] = clusters

        logger.info('''Your largest topology:
                        GlusterFS DCs: %s
                        n_gluster_per_dc: %s''' % (
                                                    len(self.configs['exp_env']['clusters']),
                                                    max(self.normalized_parameters['n_nodes_per_dc'])
                                                    )
                    )

        logger.info('Creating the combination list')
        sweeper = create_paramsweeper(result_dir=self.configs['exp_env']['results_dir'],
                                      parameters=self.normalized_parameters)
        return sweeper

    def run(self):
        sweeper = self.create_combination_queue()

        gluster_volume_name = 'gluster_volume'
        gluster_mountpoint = '/mnt/glusterd-$(hostname)'
        node_ids = None
        while len(sweeper.get_remaining()) > 0:
            if node_ids is None:
                node_ids, driver = self.setup_env()
            comb = sweeper.get_next()
            sweeper = self.run_exp_workflow(comb=comb,
                                            sweeper=sweeper,
                                            gluster_mountpoint=gluster_mountpoint,
                                            gluster_volume_name=gluster_volume_name)
            logger.info('==================================================')
            logger.info('Checking whether all provisioned nodes are running')
            is_nodes_alive, _ = is_node_active(node_ids=node_ids, 
                                            project_id=self.configs['project_id'], 
                                            driver=driver)
            if not is_nodes_alive:
                logger.info('Deleting old provisioned nodes')
                delete_ovh_nodes(node_ids=node_ids,
                                project_id=self.configs['project_id'], 
                                driver=driver)
                node_ids = None

        logger.info('Finish the experiment!!!')
        logger.info('The provisioned nodes keep running')
        if not self.args.keep_nodes:
            logger.info('Deleting provisioned nodes after finishing the experiment')
            delete_ovh_nodes(node_ids=node_ids,
                        project_id=self.configs['project_id'], 
                        driver=driver)


if __name__ == "__main__":
    logger.info("Init engine in %s" % __file__)
    engine = glusterfs_eval_ovh()

    try:
        logger.info("Start engine in %s" % __file__)
        engine.start()
    except Exception as e:
        logger.error(
            'Program is terminated by the following exception: %s' % e, exc_info=True)
        traceback.print_exc()
    except KeyboardInterrupt:
        logger.info('Program is terminated by keyboard interrupt.')
