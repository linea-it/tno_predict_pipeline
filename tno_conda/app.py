from parsl import bash_app


@bash_app
def run_pipeline(args, stderr="std.err", stdout="std.out"):
    return "/bin/bash --login {}/run.sh {} {} {} {} {}".format(
        args[5], args[0], args[1], args[2], args[3], args[4]
    )


@bash_app
def test(script, stderr="std.err", stdout="std.out"):
    return "/bin/bash --login {}".format(script)
