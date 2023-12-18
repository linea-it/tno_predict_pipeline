from parsl.config import Config
from parsl.providers import LocalProvider
from parsl.executors import HighThroughputExecutor


def get_config(key):
    """
    Creates an instance of the Parsl configuration

    Args:
        key (str): The key of the configuration to be returned.

    Returns:
        config: Parsl config instance.
    """

    executors = {
        "local": HighThroughputExecutor(
            label="local",
            worker_debug=False,
            max_workers=4,
            provider=LocalProvider(
                min_blocks=1,
                init_blocks=1,
                max_blocks=1,
                parallelism=1,
                worker_init=f"source ~/.bashrc\n",
            ),
        ),
    }

    return Config(executors=[executors[key]], strategy=None)
