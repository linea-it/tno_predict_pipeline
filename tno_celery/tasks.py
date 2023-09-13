from tno_celery.celery import app
from orbit_trace import orbit_trace_job_queue, run_job as orbit_trace_run_job
from predict_occultation import predict_job_queue, run_job as predict_run_job, check_tasks as predict_check_tasks, has_job_running, get_job_running

@app.task
def orbit_trace_run(jobid):
    print(f"Orbit trace Run Job: [{jobid}]")
    orbit_trace_run_job(jobid)


@app.task
def orbit_trace_queue():
    to_run_id = orbit_trace_job_queue()
    print(f"To run: {to_run_id}")
    if to_run_id:
        print(f"Orbit trace Job to run: [{to_run_id}]")
        orbit_trace_run.delay(to_run_id)
    return to_run_id

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
        predict_occultation_run.delay(to_run_id)

    return to_run_id

@app.task
def predict_occultation_check_tasks(jobid):
    print(f"Predict Check Task: [{jobid}]")
    return predict_check_tasks(jobid)

@app.task
def predict_occultation_running():
    running_id = get_job_running()
    print(f"Running: {running_id}")
    if running_id:
        print(f"Predict Job Running: [{running_id}]")
        predict_occultation_check_tasks.delay(running_id)

    return running_id


