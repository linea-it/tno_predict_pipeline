from parsl.config import Config
from parsl.providers import SlurmProvider, LocalProvider
from parsl.channels import SSHChannel
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher
import os


def get_config(key):
    """
    Creates an instance of the Parsl configuration

    Args:
        key (str): The key of the configuration to be returned.
            Options are: 'local' or 'linea'.

    Returns:
        config: Parsl config instance.
    """

    pipe_dir = os.getenv("WORKFLOW_PATH", ".")
    slurm_dir = os.getenv("SLURM_REMOTE_DIR", pipe_dir)
    sshkey = os.getenv("SSHKEY", "~/.ssh/id_rsa")

    executors = {
        "linea": HighThroughputExecutor(
            label="linea",
            worker_logdir_root=f'{"/".join([slurm_dir, "script_dir"])}',
            provider=SlurmProvider(
                partition="cpu",
                nodes_per_block=2,  # number of nodes
                cmd_timeout=240,  # duration for which the provider will wait for a command to be invoked on a remote system
                launcher=SrunLauncher(debug=True, overrides=""),
                init_blocks=2,
                min_blocks=3,
                max_blocks=12,
                parallelism=1,
                walltime='15:00:00',
                worker_init=f'source {"/".join([slurm_dir, "cluster.sh"])}\n',
                channel=SSHChannel(
                    hostname="loginapl01",
                    username="app.tno",
                    key_filename=sshkey,
                    script_dir=f'{"/".join([slurm_dir, "script_dir"])}',
                )
            ),
        ),
        "local": HighThroughputExecutor(
            label="local",
            worker_debug=False,
            max_workers=4,
            provider=LocalProvider(
                min_blocks=1,
                init_blocks=1,
                max_blocks=1,
                parallelism=1,
                worker_init=f"source /app/src/env.sh\n",
            ),
        ),
    }

    return Config(executors=[executors[key]], strategy=None)
