import json
import urllib
import os

# import configparser
import io
import warnings


class Condor:
    def __init__(self):

        try:
            self.cluster_name = "ICE-X"
            self.condor_scheduler = "loginicx.linea.gov.br"
            self.condor_version = "8.8.1"

        except Exception as e:
            raise e
            raise SystemExit


    def parse_requirements(self, args):
        requirements = ""
        t = 0
        for arg in args:
            requirements += arg + "==" + str(args[arg])
            if t <= len(args) - 2:
                requirements += "&&"
                t = t + 1
        return requirements

    def parse_job_to_dict(self, job):
        j = dict()

        for key in job.keys():
            j[key] = str(job.get(key))

        return j

