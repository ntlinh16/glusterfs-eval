import traceback
import random


from cloudal.action import performing_actions_g5k
from cloudal.configurator import filebench_configurator, glusterfs_configurator, CancelException
from cloudal.experimenter import is_job_alive, get_results, define_parameters, create_paramsweeper
from cloudal.provisioner import g5k_provisioner
from cloudal.utils import get_logger, execute_cmd, parse_config_file, ExecuteCommandException

from - import slugify
from execo_g5k import oardel


logger = get_logger()

class glusterfs_eval_g5k(performing_actions_g5k):
    def __init__(self, **kwargs):
        super(glusterfs_eval_g5k, self).__init__()
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

    def run_benchmark(self, comb, hosts, gluster_mountpoint):
        benchmark = comb['benchmarks']
        logger.info('--------------------------------------')
        logger.info("3. Starting benchmark: %s" % benchmark)
        gluster_mountpoint = 'mnt\/glusterd-$(hostname)'
        filebench_hosts = random.sample(hosts, comb['n_clients'])
        cmd = "hostname -I | awk '{print $1}'"
        _,ips = execute_cmd(cmd, filebench_hosts)
        ip_hosts =list()
        for ip in ips.processes:
            ip_hosts.append(ip.stdout.strip())
        configurator = filebench_configurator()
        if benchmark == 'mailserver':
            is_finished = configurator.run_mailserver(ip_hosts, gluster_mountpoint, comb['duration'], comb['n_threads'])
            return is_finished, filebench_hosts

    def deploy_glusterfs(self, indices, gluster_mountpoint, gluster_volume_name):
        logger.info('--------------------------------------')
        logger.info("2. Deploying GlusterFS")
        configurator = glusterfs_configurator()
        return configurator.deploy_glusterfs(self.ip_hosts, indices, gluster_mountpoint, gluster_volume_name)


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
        comb_ok = False
        try:
            logger.info('=======================================')
            logger.info('Performing combination: ' + slugify(comb))

            self.clean_exp_env(self.ip_hosts, gluster_mountpoint, gluster_volume_name)
            # Get indices to select the number of GlusterFS nodes from list of hosts
            indices = random.sample(range(len(self.ip_hosts)), comb['n_nodes_per_dc'] * comb['n_dc'])
            glusterfs, ip_hosts = self.deploy_glusterfs(indices, gluster_mountpoint, gluster_volume_name)
            if glusterfs:
                is_finished, result_hosts = self.run_benchmark(comb, ip_hosts, gluster_mountpoint)
                if is_finished:
                    self.save_results(comb, result_hosts)
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

    def config_host(self):
        logger.info("Starting configuring nodes")
        configurator = filebench_configurator()
        configurator.install_filebench(self.ip_hosts)

        configurator = glusterfs_configurator()
        configurator.install_glusterfs(self.ip_hosts)
        logger.info("Finish configuring nodes")

    def setup_env(self):
        logger.info("Starting configuring the experiment environment")
        logger.debug("Init provisioner: g5k_provisioner")
        provisioner = g5k_provisioner(configs=self.configs,
                                      keep_alive=self.args.keep_alive,
                                      out_of_chart=self.args.out_of_chart,
                                      oar_job_ids=self.args.oar_job_ids,
                                      no_deploy_os=self.args.no_deploy_os,
                                      is_reservation=self.args.is_reservation,
                                      job_name="cloudal_glusterfs",)

        provisioner.provisioning()
        self.hosts = provisioner.hosts
        oar_job_ids = provisioner.oar_result
        self.oar_result = provisioner.oar_result

        cmd = "hostname -I | awk '{print $1}'"
        _,ips = execute_cmd(cmd, self.hosts)
        self.ip_hosts =list()
        for ip in ips.processes:
            self.ip_hosts.append(ip.stdout.strip())


        if not self.args.no_config_host:
            self.config_host()

        self.args.oar_job_ids = None
        logger.info("FINISH SETTING THE EXPERIMENT ENVIRONMENT\n")
        return oar_job_ids

    
    def create_combination_queue(self):
        logger.debug('Parse and convert configs for OVH provisioner')
        self.configs = parse_config_file(self.args.config_file_path)

        # Add the number of gluster DC as a parameter
        self.configs['parameters']['n_dc'] = len(self.configs['exp_env']['clusters'])

        logger.debug('Normalize the parameter space')
        self.normalized_parameters = define_parameters(self.configs['parameters'])

        logger.debug('Normalize the given configs')
        n_nodes_per_cluster = max(self.normalized_parameters['n_nodes_per_dc'])
        # create standard cluster information to make reservation on Grid'5000, this info using by G5k provisioner
        clusters = list()
        for cluster in self.configs['exp_env']['clusters']:
            clusters.append({'cluster': cluster, 'n_nodes': n_nodes_per_cluster})
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
        oar_job_ids = None
        while len(sweeper.get_remaining()) > 0:
            if oar_job_ids is None:
                oar_job_ids = self.setup_env()

            comb = sweeper.get_next()
            sweeper = self.run_exp_workflow(comb=comb,
                                        sweeper=sweeper,
                                        gluster_mountpoint=gluster_mountpoint,
                                        gluster_volume_name=gluster_volume_name)
            logger.info('==================================================')
            logger.info('Checking whether all provisioned nodes are running')
            if not is_job_alive(oar_job_ids):
                logger.info('Deleting old provisioned nodes')
                oardel(oar_job_ids)
                oar_job_ids = None
        logger.info("Finish the experiment!!!")


if __name__ == "__main__":
    logger.info("Init engine in %s" % __file__)
    engine = glusterfs_eval_g5k()

    try:
        logger.info("Start engine in %s" % __file__)
        engine.start()
    except Exception as e:
        logger.error(
            'Program is terminated by the following exception: %s' % e, exc_info=True)
        traceback.print_exc()
    except KeyboardInterrupt:
        logger.info('Program is terminated by keyboard interrupt.')
