import fastapi
from fastapi import Request
import pandas
from task.task import Task

app = fastapi.FastAPI()

# Assuming the json input data format to be of the form json = {idempotency_key = "idempotency_key", data:[{country:"country_name, states:["state1"]}, ]}

@app.post("/job")
def start_job():
    body = Request.body
    key = body.get("idempotency_key")
    if not key:
        return "Invalid input. Idempotency key required"
    query = "select * from job where idempotency_key= ?"
    params = (key)
    sql_search = sql.execute(query = query, params = params)
    if sql_search is None:
        # Generating job id
        insert_query = f"insert into job(idempotency_key) select {key}"
        sql.execute(query = insert_query)

        # Fetching the newly generated job id
        query = "select * from job where idempotency_key= ?"
        params = (key)
        sql_search = sql.execute(query = query, params = params).fetchone()
        job_id = sql_search.id
        # Code for parallel processing country level tasks
        # We will call the function that parallely processes functions with multiple inputs as process_parallel_task
        process_list = []
        input_data = body.get("data")
        for country_input in input_data:
            process_list.append(process_parallel_task(Task(job_id=job_id, key=key, input = country_input)))

        # execute the parallel process

        return job_id
    else:
        job_id = sql_search.id
        # Code for parallel processing country level tasks
        # We will call the function that parallely processes functions with multiple inputs as process_parallel_task
        process_list = []
        input_data = body.get("data")
        for country_input in input_data:
            process_list.append(process_parallel_task(Task(job_id=job_id, key=key, input = country_input)))

        # execute the parallel process

        return job_id
    
@app.get("/{job_id}")
def get_job_status(job_id):
    query = "select idempotency_key, country, state, status_code from task where job_id = ?"
    params = (job_id)
    job_status_report = sql.execute(query = query, params = params)
    df = pandas.read_sql(job_status_report)
    return df.to_json()
