import traceback
import random

from cloudal.utils import get_logger, execute_cmd, parse_config_file, ExecuteCommandException
from cloudal.action import performing_actions
from cloudal.provisioner import ovh_provisioner
from cloudal.configurator import packages_configurator, CancelException
from cloudal.experimenter import create_paramsweeper, define_parameters, get_results

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

    def save_results(self, comb, hosts):
        logger.info("----------------------------------")
        logger.info("4. Starting dowloading the results")
        get_results(comb=comb,
                    hosts=hosts,
                    remote_result_files=['/tmp/results/*'],
                    local_result_dir=self.configs['exp_env']['results_dir'])

    def run_mailserver(self, gluster_hosts, duration, n_client):
        if n_client == 100:
            n_hosts = 1
        else:
            n_hosts = n_client

        hosts = random.sample(gluster_hosts, n_hosts)
        logger.info('Dowloading Filebench configuration file')
        cmd = 'wget https://raw.githubusercontent.com/filebench/filebench/master/workloads/varmail.f -P /tmp/ -N'
        execute_cmd(cmd, hosts)

        logger.info('Editing the configuration file')
        cmd = 'sed -i "s/tmp/tmp\/dc-$(hostname)/g" /tmp/varmail.f'
        execute_cmd(cmd, hosts)
        cmd = 'sed -i "s/run 60/run %s/g" /tmp/varmail.f' % duration
        execute_cmd(cmd, hosts)
        cmd = 'sed -i "s/name=bigfileset/name=bigfileset-$(hostname)/g" /tmp/varmail.f'
        execute_cmd(cmd, hosts)
        cmd = 'sed -i "s/meandirwidth=1000000/meandirwidth=1000/g" /tmp/varmail.f'
        execute_cmd(cmd, hosts)
        if n_client != 100:
            cmd = 'sed -i "s/nthreads=16/nthreads=32/g" /tmp/varmail.f'
            execute_cmd(cmd, hosts)

        logger.info('Clearing cache ')
        cmd = 'rm -rf /tmp/dc-$(hostname)/bigfileset'
        execute_cmd(cmd, hosts)
        cmd = 'sync; echo 3 > /proc/sys/vm/drop_caches'
        execute_cmd(cmd, hosts)

        logger.info('hosts = %s' % hosts)
        logger.info('Running filebench in %s second' % duration)
        cmd = 'setarch $(arch) -R filebench -f /tmp/varmail.f > /tmp/results/filebench_$(hostname)'
        execute_cmd(cmd, hosts)
        return True, hosts

    def run_benchmark(self, comb, glusterfs_hosts):
        benchmark = comb['benchmarks']
        logger.info('--------------------------------------')
        logger.info("3. Starting benchmark: %s" % benchmark)
        if benchmark == "mailserver":
            return self.run_mailserver(glusterfs_hosts, comb['duration'], comb['n_client'])

    def deploy_glusterfs(self, hosts, gluster_volume_name):
        logger.info('--------------------------------------')
        logger.info("2. Deploying GlusterFS")
        logger.info('Creating volumes on hosts')
        volume_path = '/tmp/glusterd/volume'
        cmd = 'mkdir -p %s' % volume_path
        execute_cmd(cmd, hosts)

        volume_params = list()
        for index, node in enumerate(self.data_nodes):
            volume_params.append("gluster-%s.%s.local:%s" % (index, node['name'], volume_path))
        volume_params = " ".join(volume_params)

        cmd ='gluster --mode=script volume create %s replica 3 %s' % (gluster_volume_name, volume_params)
        execute_cmd(cmd, self.data_nodes[0]['ipAddresses'][0]['ip'])
        
        logger.info('Starting volumes on hosts')
        cmd = 'gluster --mode=script volume start %s' % gluster_volume_name
        execute_cmd(cmd, self.data_nodes[0]['ipAddresses'][0]['ip'])

        cmd = ''' mkdir -p /mnt/glusterd-$(hostname) &&
                  mount -t glusterfs gluster-0:/%s /mnt/glusterd-$(hostname)''' % gluster_volume_name
        execute_cmd(cmd, hosts)
        logger.info("Finish deploying glusterfs")
        return True


    def clean_exp_env(self, hosts, gluster_volume_name):
        logger.info('1. Cleaning experiment environment ')
        if len(hosts) > 0:
            logger.info('Delete all files in /tmp/results folder on %s glusterfs nodes' % len(hosts))
            cmd = 'rm -rf /tmp/results && mkdir -p /tmp/results'
            execute_cmd(cmd, hosts)
            
            logger.info('Delete all data on %s glusterfs nodes' % len(hosts))
            for host in hosts:
                cmd = "mount | grep /mnt/glusterd-$(hostname)"
                _, r = execute_cmd(cmd, host)
                is_mount = r.processes[0].stdout.strip()

                if is_mount:
                    execute_cmd(cmd, host)
                    cmd = '''umount /mnt/glusterd-$(hostname) &&
                            rm -rf /mnt/glusterd-$(hostname)/* '''
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

    def run_exp_workflow(self, comb, sweeper, gluster_volume_name):
        comb_ok = False
        try:
            logger.info('=======================================')
            logger.info('Performing combination: ' + slugify(comb))

            self.clean_exp_env(self.data_hosts, gluster_volume_name)
            glusterfs = self.deploy_glusterfs(self.data_hosts, gluster_volume_name)
            if glusterfs:
                is_finished, hosts = self.run_benchmark(comb, self.data_hosts)
                if is_finished:
                    self.save_results(comb, hosts)
                    comb_ok = True
            else:
                raise CancelException("Cannot deploy glusterfs")
        except (ExecuteCommandException, CancelException) as e:
            logger.error('Combination exception: %s' % e)
            comb_ok = False
        finally:
            if comb_ok:
                sweeper.done(comb)
                logger.info('Finish combination: %s' % slugify(comb))
            else:
                sweeper.cancel(comb)
                logger.warning(slugify(comb) + ' is canceled')
            logger.info('%s combinations remaining\n' % len(sweeper.get_remaining()))
        return sweeper

    def install_filebench(self, hosts):
        logger.info('Installing Filebench')
        configurator = packages_configurator()
        configurator.install_packages(["build-essential", "bison", "flex", "libtool"], hosts)

        cmd = "wget https://github.com/filebench/filebench/archive/refs/tags/1.5-alpha3.tar.gz -P /tmp/ -N"
        execute_cmd(cmd, hosts)
        cmd = "tar -xf /tmp/1.5-alpha3.tar.gz --directory /tmp/"
        execute_cmd(cmd, hosts)
        cmd = '''cd /tmp/filebench-1.5-alpha3/ &&
                 libtoolize &&
                 aclocal &&
                 autoheader &&
                 automake --add-missing &&
                 autoconf &&
                 ./configure &&
                 make &&
                 make install'''
        execute_cmd(cmd, hosts)

    def install_gluster(self, hosts):
        logger.info('Installing GlusterFS')
        configurator = packages_configurator()
        configurator.install_packages(["glusterfs-server"], hosts)
        
        cmd = 'systemctl start glusterd'
        execute_cmd(cmd, hosts)

        gluster_configuration = list()
        for index, node in enumerate(self.data_nodes):
            gluster_configuration.append("%s gluster-%s.%s.local gluster-%s " % (
                                            node['ipAddresses'][0]['ip'], index, node['name'], index))
        gluster_configuration = "\n".join(gluster_configuration)
        cmd = "echo '%s' >> /etc/hosts" % gluster_configuration
        execute_cmd(cmd, hosts)

        for index, _ in enumerate(self.data_nodes):
            cmd = 'gluster peer probe gluster-%s' % index
            execute_cmd(cmd, self.data_nodes[0]['ipAddresses'][0]['ip'])


    def config_host(self):
        logger.info("Starting configuring nodes")
        self.install_gluster(self.hosts)
        self.install_filebench(self.hosts)
        logger.info("Finish configuring nodes")

    def setup_env(self):
        logger.info("STARTING SETTING THE EXPERIMENT ENVIRONMENT")
        logger.info("Starting provisioning nodes on OVHCloud")

        provisioner = ovh_provisioner(configs=self.configs, node_ids_file=self.args.node_ids_file)
        provisioner.provisioning()

        self.nodes = provisioner.nodes
        self.hosts = provisioner.hosts
        node_ids_file = provisioner.node_ids_file

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

        logger.info("FINISH SETTING THE EXPERIMENT ENVIRONMENT\n")
        return node_ids_file

    def create_configs(self):
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

    def run(self):
        logger.debug('Parse and convert configs for OVH provisioner')
        self.configs = parse_config_file(self.args.config_file_path)
        # Add the number of gluster DC as a parameter
        self.configs['parameters']['n_dc'] = len(self.configs['exp_env']['clusters'])

        logger.debug('Normalize the parameter space')
        self.normalized_parameters = define_parameters(self.configs['parameters'])

        logger.debug('Normalize the given configs')
        self.create_configs()

        logger.info('''Your largest topology:
                        gluster DCs: %s
                        n_gluster_per_dc: %s''' % (
                                                    len(self.configs['exp_env']['clusters']),
                                                    max(self.normalized_parameters['n_nodes_per_dc'])
                                                    )
                    )

        logger.info('Creating the combination list')
        sweeper = create_paramsweeper(result_dir=self.configs['exp_env']['results_dir'],
                                      parameters=self.normalized_parameters)

        gluster_volume_name = 'gluster_volume'
        node_ids_file = None
        while len(sweeper.get_remaining()) > 0:
            if node_ids_file is None:
                node_ids_file = self.setup_env()
            comb = sweeper.get_next()
            sweeper = self.run_exp_workflow(comb=comb,
                                            sweeper=sweeper,
                                            gluster_volume_name=gluster_volume_name)
            # if not is_nodes_alive(node_ids_file):
            #     node_ids_file = None
        logger.info('Finish the experiment!!!')


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
