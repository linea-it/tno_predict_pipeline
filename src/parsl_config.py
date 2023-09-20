from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.executors import HighThroughputExecutor
from parsl.monitoring.monitoring import MonitoringHub
from parsl.providers import LocalProvider, CondorProvider
from parsl.channels import SSHChannel
from parsl.addresses import address_by_hostname, address_by_query

# PARSL Config
htex_config = Config(
    executors=[
        HighThroughputExecutor(
            label="htcondor",
            address=address_by_query(),
            max_workers=1,
            provider=CondorProvider(
                # Total de blocks no condor 896
                # init_blocks=600,
                # min_blocks=1,
                # max_blocks=896,
                init_blocks=600,
                min_blocks=1,
                max_blocks=800,
                parallelism=1,
                scheduler_options='+AppType = "TNO"\n+AppName = "Orbit Trace"\n',
                # worker_init="source /archive/cl/ton/dev/pipelines/env.sh",
                worker_init="pwd && source /lustre/t1/tmp/tno/pipelines/env.sh",
                cmd_timeout=120,
            ),
        ),
    ],
    strategy=None,
)
