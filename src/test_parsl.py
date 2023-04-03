# import parsl
# from parsl.config import Config
# from parsl.channels import LocalChannel
# from parsl.executors import HighThroughputExecutor
# from parsl.monitoring.monitoring import MonitoringHub
# from parsl.providers import LocalProvider, CondorProvider
# from parsl.channels import SSHChannel
# from parsl.addresses import address_by_hostname, address_by_query


# htex_config = Config(
#     executors=[
#         HighThroughputExecutor(
#             label='htcondor',
#             address=address_by_query(),
#             max_workers=1,
#             provider=CondorProvider(
#                 # init_blocks=500,
#                 init_blocks=5,
#                 min_blocks=1,
#                 # max_blocks=500,
#                 max_blocks=5,
#                 parallelism=1,
#                 scheduler_options='+AppType = "TNO"\n+AppName = "Orbit Trace"\n',
#                 worker_init="source /archive/des/tno/dev/nima/pipeline/env.sh",
#                 cmd_timeout=120,
#                 # channel=SSHChannel(
#                 #     hostname="loginicx",
#                 #     username="glauber.costa",
#                 #     password="EDwsQA877021",
#                 #     script_dir="/archive/des/tno/dev/nima/pipeline/"
#                 # )
#             )
#         ),
#     ],
#     strategy=None,
# )


# # Load Parsl Configs
# parsl.clear()
# parsl.load(htex_config)
