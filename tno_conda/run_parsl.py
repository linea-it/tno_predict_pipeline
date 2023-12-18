from app import test
from parsl_config import get_config

import parsl


def run_test():
    parsl_conf = get_config("local")
    parsl.clear()
    parsl.load(parsl_conf)

    proc = test(
        "/app/pipe2.7/pipeline.sh",
        stderr="/app/example/test.err",
        stdout="/app/example/test.out",
    )

    proc.result()

    print("Test executed!")


if __name__ == "__main__":
    run_test()
