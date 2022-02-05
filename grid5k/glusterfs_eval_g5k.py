import traceback
import random
from cloudal.utils import get_logger, execute_cmd, parse_config_file, ExecuteCommandException
from cloudal.action import performing_actions_g5k
from cloudal.provisioner import g5k_provisioner
from cloudal.configurator import packages_configurator, CancelException
from cloudal.experimenter import is_job_alive, get_results, define_parameters, create_paramsweeper

from execo_engine import slugify
from execo_g5k import oardel


logger = get_logger()

class glusterfs_eval_g5k(performing_actions_g5k):
    def __init__(self, **kwargs):
        super(glusterfs_eval_g5k, self).__init__()
        self.args_parser.add_argument("--no-config-host", dest="no_config_host",
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
            is_finished, hosts = self.run_mailserver(glusterfs_hosts, comb['duration'], comb['n_client'])
            return is_finished, hosts

    def deploy_glusterfs(self, indices, gluster_volume_name):
        logger.info('--------------------------------------')
        logger.info("2. Deploying GlusterFS")
        indices = sorted(indices)
        hosts = [host for index,host in enumerate(self.hosts) if index in indices]
        logger.info('Creating volumes on %s hosts' % len(hosts))
        logger.info('Creating volumes on hosts: %s' % hosts)
        volume_path = '/tmp/glusterd/volume'
        cmd = 'mkdir -p %s' % volume_path
        execute_cmd(cmd, hosts)

        volume_params = list()
        for index, host in zip(indices, hosts):
            volume_params.append("gluster-%s.%s.local:%s" % (index, host, volume_path))
        volume_params = " ".join(volume_params)

        cmd ='gluster --mode=script volume create %s replica 3 %s' % (gluster_volume_name, volume_params)
        execute_cmd(cmd, hosts[0])
        
        logger.info('Starting volumes on hosts')
        cmd = 'gluster --mode=script volume start %s' % gluster_volume_name
        execute_cmd(cmd, hosts[0])

        cmd = ''' mkdir -p /mnt/glusterd-$(hostname) &&
                  mount -t glusterfs gluster-0:/%s /mnt/glusterd-$(hostname)''' % gluster_volume_name
        execute_cmd(cmd, hosts)
        logger.info("Finish deploying glusterfs")
        return True, hosts


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

            self.clean_exp_env(self.hosts, gluster_volume_name)
            # Get indices to select the number of n_gluster_per_dc from list of hosts
            indices = random.sample(range(len(self.hosts)), comb['n_nodes_per_dc'])
            glusterfs, hosts = self.deploy_glusterfs(indices, gluster_volume_name)
            if glusterfs:
                is_finished, hosts = self.run_benchmark(comb, hosts)
                if is_finished:
                    comb_ok = True
                    self.save_results(comb, hosts)
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
        for index, host in enumerate(hosts):
            cmd = "hostname -I | awk '{print $1}'"
            _, r = execute_cmd(cmd, host)
            host_ip = r.processes[0].stdout.strip()
            gluster_configuration.append("%s gluster-%s.%s.local gluster-%s " % (host_ip, index, host, index))
        gluster_configuration = "\n".join(gluster_configuration)
        cmd = "echo '%s' >> /etc/hosts" % gluster_configuration
        execute_cmd(cmd, hosts)

        for index, _ in enumerate(hosts):
            cmd = 'gluster peer probe gluster-%s' % index
            execute_cmd(cmd, hosts[0])


    def config_host(self):
        logger.info("Starting configuring nodes")
        self.install_gluster(self.hosts)
        self.install_filebench(self.hosts)
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
                                      job_name="cloudal",)

        provisioner.provisioning()
        self.hosts = provisioner.hosts
        oar_job_ids = provisioner.oar_result
        self.oar_result = provisioner.oar_result

        if not self.args.no_config_host:
            self.config_host()

        self.args.oar_job_ids = None
        logger.info("FINISH SETTING THE EXPERIMENT ENVIRONMENT\n")
        return oar_job_ids

    def create_configs(self):
        n_nodes_per_cluster = max(self.normalized_parameters['n_nodes_per_dc'])

        # create standard cluster information to make reservation on Grid'5000, this info using by G5k provisioner
        clusters = list()
        for cluster in self.configs['exp_env']['clusters']:
            clusters.append({'cluster': cluster, 'n_nodes': n_nodes_per_cluster})
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
        oar_job_ids = None
        while len(sweeper.get_remaining()) > 0:
            if oar_job_ids is None:
                oar_job_ids = self.setup_env()

            comb = sweeper.get_next()
            sweeper = self.run_exp_workflow(comb=comb,
                                        sweeper=sweeper,
                                        gluster_volume_name=gluster_volume_name)

            if not is_job_alive(oar_job_ids):
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
