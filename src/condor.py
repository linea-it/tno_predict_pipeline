import htcondor
import classad
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

    def get_jobs(self, args, cols):

        self.default_params = [
            "Args",
            "GlobalJobId",
            "JobStartDate",
            "JobStatus",
            "Out",
            "Owner",
            "RemoteHost",
            "RequestCpus",
            "RequiresWholeMachine",
            "UserLog",
        ]
        self.params = self.default_params + str(cols).split(",")
        self.requirements = ""

        if len(args):

            t = 0
            for arg in args:
                self.requirements += arg + "==" + str(args[arg])
                if t <= len(args) - 2:
                    self.requirements += "&&"
                    t = t + 1
        else:
            self.requirements = None

        self.jobs = []

        if self.condor_version >= "8.8.1":

            try:
                for schedd_ad in htcondor.Collector().locateAll(
                    htcondor.DaemonTypes.Schedd
                ):
                    self.schedd = htcondor.Schedd(schedd_ad)
                    self.jobs += self.schedd.xquery(
                        projection=self.params, constraint=self.requirements
                    )
            except Exception as e:
                print(str(e))
                raise e

        else:
            condor_q = os.popen("condor_q -l -global")
            ads = classad.parseOldAds(condor_q)
            for ad in ads:
                self.jobs.append(ad)

        self.job_procs = {}
        self.info = {}

        rows = list()

        for job in range(len(self.jobs)):

            process = None
            if "Args" in self.jobs[job]:
                process = self.jobs[job]["Args"].split(" ")[0]
            else:
                process = " "
            if "GlobalJobId" in self.jobs[job]:
                jobid = self.jobs[job]["GlobalJobId"]
            else:
                jobid = " "
            if "Owner" in self.jobs[job]:
                self.info["owner"] = self.jobs[job]["Owner"]
            else:
                self.info["owner"] = " "

            row = dict(
                {"Process": process, "Job": jobid, "ClusterName": self.cluster_name}
            )

            for info in self.jobs[job]:

                row[info] = str(self.jobs[job][info])

            rows.append(row)

        return rows

    def get_nodes(self, match, *args):

        self.default_params = [
            "UtsnameNodename",
            "Name",
            "State",
            "Memory",
            "Disk",
            "TotalCpus",
            "RemoteOwner",
            "LoadAvg",
            "Activity",
            "JobStarts",
            "RecentJobStarts",
            "DiskUsage",
        ]
        self.params = self.default_params + args[0]
        self.requirements = (
            str(match).replace("=", "==").replace(",", "&&") if match else None
        )
        self.rows = list()

        coll = htcondor.Collector()
        query = coll.query(htcondor.AdTypes.Startd, projection=self.params)

        for node in range(len(query)):
            row = dict()

            for key in query[node].keys():
                row[key] = str(query[node].get(key))

            self.rows.append(row)

        return self.rows

    def get_history(self, args, cols, limit):

        requirements = self.parse_requirements(args)
        if requirements is "":
            requirements = "true"

        projection = cols

        rows = list()

        schedd = htcondor.Schedd()
        for job in schedd.history(requirements, projection, limit):

            if len(job):
                rows.append(self.parse_job_to_dict(job))

        return rows

    def submit_job(self, params):

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            # TODO tratar execessao nesta funcao.
            n_queues = params.get("queues", 1)

            submit_param = params.get("submit_params", None)
            if submit_param is None:
                return dict(
                    {
                        "success": False,
                        "message": "NENHUM PARAMETRO DE SUBMISSAO FOI ENVIADO.",
                    }
                )

            schedd = htcondor.Schedd()
            sub = htcondor.Submit(submit_param)

            with schedd.transaction() as txn:
                clusterId = sub.queue(txn, n_queues)

            # Listar os jobs
            jobs = list()
            for job in schedd.xquery(
                projection=["ClusterId", "ProcId", "JobStatus"],
                constraint="ClusterId==%s" % clusterId,
            ):
                # print(self.parse_job_to_dict(job))
                jobs.append(self.parse_job_to_dict(job))

            return dict({"success": True, "jobs": jobs})

    def remove_job(self, clusterId, procId):
        # print("Removing Job ClusterId: [%s] ProcId: [%s]" % (clusterId, procId))

        schedd = htcondor.Schedd()

        try:
            schedd.act(
                htcondor.JobAction.Remove,
                "ClusterId==%s && ProcId==%s" % (clusterId, procId),
            )

            job = self.get_job(clusterId, procId, ["ClusterId", "ProcId", "JobStatus"])

            return dict({"success": True, "job": job})
        except:
            return dict({"success": False})

    def get_job(self, clusterId, procId, projection=[]):

        schedd = htcondor.Schedd()

        requirements = "ClusterId==%s && ProcId==%s" % (clusterId, procId)

        jobs = list()
        for job in schedd.xquery(constraint=requirements, projection=projection):
            jobs.append(self.parse_job_to_dict(job))

        if len(jobs) == 0:
            # Tenta recuperar o job do historico
            for job in schedd.history(requirements, projection, 1):

                if len(job) > 0:
                    return self.parse_job_to_dict(job)
                else:
                    return None

        elif len(jobs) == 1:
            return jobs[0]
        else:
            return jobs

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

