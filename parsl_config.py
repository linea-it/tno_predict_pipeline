from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.executors import HighThroughputExecutor
from parsl.monitoring.monitoring import MonitoringHub
from parsl.providers import LocalProvider, CondorProvider
from parsl.channels import SSHChannel
from parsl.addresses import address_by_hostname, address_by_query


# Diretório Raiz onde estão os arquivos de catalog
DES_CATALOGS_BASEPATH = '/archive/des/public/catalogs'

# PARSL Config
# htex_config = Config(
#    executors=[
#        HighThroughputExecutor(
#            label="htex_local",
#            cores_per_worker=1,
#            max_workers=16,
#            provider=LocalProvider(
#                channel=LocalChannel(),
#                worker_init=f"source /home/appuser/orbit_trace/env.sh",
#            ),
#        )
#    ],
# )

htex_config = Config(
    executors=[
        # HighThroughputExecutor(
        #     label='htcondor',
        #     address=address_by_query(),
        #     max_workers=56,
        #     # cores_per_worker=1.2,
        #     provider=CondorProvider(
        #         # cores_per_slot=1,
        #         init_blocks=1,
        #         # min_blocks=10,
        #         # max_blocks=20,
        #         parallelism=1,
        #         # scheduler_options='+RequiresWholeMachine = True',
        #         scheduler_options='+AppType = "TNO"\n+AppName = "Orbit Trace"\n',
        #         worker_init="source /archive/des/tno/development/orbit_trace/env.sh",
        #         cmd_timeout=120,
        #         channel=SSHChannel(
        #             hostname="loginicx",
        #             username="<USERNAME>",
        #             password="<PASSWORD>",
        #             script_dir="/archive/des/tno/development/orbit_trace/"
        #         )
        #     )
        # ), 23H
        HighThroughputExecutor(
            label='htcondor',
            address=address_by_query(),
            max_workers=1,
            # cores_per_worker=1.2,
            provider=CondorProvider(
                # cores_per_slot=1,
                init_blocks=500,
                min_blocks=1,
                max_blocks=500,
                parallelism=1,
                # scheduler_options='+RequiresWholeMachine = True',
                scheduler_options='+AppType = "TNO"\n+AppName = "Orbit Trace"\n',
                worker_init="source /archive/des/tno/dev/nima/pipeline/env.sh",
                cmd_timeout=120,
                channel=SSHChannel(
                    hostname="loginicx",
                    username="<USERNAME>",
                    password="<PASSWORD>",
                    script_dir="/archive/des/tno/dev/nima/pipeline/"
                )
            )
        ),
        #     HighThroughputExecutor(
        #         label='htcondor_8',
        #         address=address_by_query(),
        #         max_workers=1,
        #         # cores_per_worker=1.2,
        #         provider=CondorProvider(
        #             init_blocks=1,
        #             # cores_per_slot=1,
        #             # min_blocks=1,
        #             # max_blocks=4,
        #             parallelism=1,
        #             # scheduler_options='+RequiresWholeMachine = True',
        #             scheduler_options='+AppType = "TNO"\n+AppName = "Orbit Trace"\n',
        #             worker_init="source /archive/des/tno/development/orbit_trace/env.sh",
        #             cmd_timeout=120,
        #             channel=SSHChannel(
        #                 hostname="loginicx",
        #                 username="<USERNAME>",
        #                 password="<PASSWORD>",
        #                 username="glauber.costa",
        #                 password="EDwsQA877021",
        #                 script_dir="/archive/des/tno/development/orbit_trace/"
        #             )
        #         )
        #     )
    ],
    # monitoring=MonitoringHub(
    #     hub_address=address_by_hostname(),
    #     hub_port=55055,
    #     monitoring_debug=True,
    #     logging_endpoint=f"sqlite:////archive/tmp/singulani/tno/tno.db",
    #     resource_monitoring_interval=10,
    # ),
    strategy=None,
    #     run_dir='/archive/tmp/singulani/tno/'
)
