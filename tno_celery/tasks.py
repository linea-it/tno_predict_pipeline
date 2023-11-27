from tno_celery.celery import app
from predict_occultation import predict_job_queue, run_job as predict_run_job
import gc

@app.task
def predict_occultation_run(jobid):
    print(f"Predict Run Job: [{jobid}]")
    return predict_run_job(jobid)

@app.task
def predict_occultation_queue():
    to_run_id = predict_job_queue()
    print(f"To run: {to_run_id}")
    if to_run_id:
        print(f"Predict Job to run: [{to_run_id}]")
        data = predict_run_job(to_run_id)
        print(f"Executed: %s" % str(data))

    gc.collect()

    return to_run_id
