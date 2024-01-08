from predict_occultation import rerun_job
from sys import argv


def main():
    if len(argv) > 1:
        jobid = argv[1]
    else:
        raise ValueError("No job")

    rerun_job(jobid)


if __name__ == "__main__": main()

