import fastapi
from fastapi import Request
from task.task import Task
import sqlite3
from pydantic import BaseModel
import json
import multiprocessing
import concurrent.futures

# Assuming the json input data format to be of the form json = {idempotency_key = "idempotency_key", data:[{country:"country_name, states:["state1"]}, ]}

class JobItem(BaseModel):
    task_data: dict | None = None


app = fastapi.FastAPI()

@app.post("/job/")
async def start_job(job_item: JobItem):

    def parallel_task(main_task, job_id, key, data):
        task = main_task(job_id, key, data)
        return task.run_task()

    body = json.loads(job_item.model_dump_json()).get("task_data")
    print(body)
    # print(type(body))
    key = body.get("idempotency_key")
    print(key)
    con = sqlite3.connect("task.db")
    cur = con.cursor()
    if not key:
        return {"message":"Invalid input. Idempotency key required"}
    # task_data = body.get("data")
    query = "select id, idempotency_key from job where idempotency_key = ?"
    params = (key, )
    sql_search = cur.execute(query, (key,), ).fetchone()
    if sql_search is None:
        # Generating job id
        insert_query = "insert into job (idempotency_key) values (?)"
        cur.execute(insert_query, (key, ), )
        con.commit()

        # Fetching the newly generated job id
        query = "select id, idempotency_key from job where idempotency_key = ?"
        params = (key,)
        sql_search = cur.execute(query, params, ).fetchone()
        job_id = sql_search[0]

        task_input_data = body.get("data")
        executor = concurrent.futures.ThreadPoolExecutor()

        # iteratively add the processes to the threadpool
        for input_data in task_input_data:
            executor.submit(Task, job_id, key, input_data)
        
        # The below code snippet will allow the api to return a result without waiting for the threads to complete processing
        executor.shutdown(wait=False)

        return {"job_id": job_id}
    else:
        job_id = sql_search[0]

        task_input_data = body.get("data")
        
        executor = concurrent.futures.ThreadPoolExecutor()

        # iteratively add the processes to the threadpool
        for input_data in task_input_data:
            executor.submit(Task, job_id, key, input_data)
        
        # The below code snippet will allow the api to return a result without waiting for the threads to complete processing
        executor.shutdown(wait=False)

        return {"job_id": job_id}
    
@app.get("/{job_id}")
async def get_job_status(job_id):
    con = sqlite3.connect("task.db")
    cur = con.cursor()
    query = "select idempotency_key, country, state, status_code from task where job_id = ?"
    params = (job_id, )
    job_status_report = cur.execute(query, params, ).fetchall()
    if job_status_report:
        json_data = {}
        count = 0

        for job in job_status_report:
            json_data[count] = {"idempotency_key": job[0], "country": job[1], "state": job[2], "status_code": job[3]}
            count += 1
        return json_data
    else:
        return {"message": "No job data found"}
