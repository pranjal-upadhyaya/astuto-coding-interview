class Task:

    def __init__(self, job_id, key, input: dict):
        country = input["country"]
        state_list = input["states"]
        self.job_id = job_id
        self.idempotency_key = key
        self.country = country
        self.state_list = state_list

    def run_sub_task(self, state: str, retries = 3):
        successfull_run = False
        if retries > 0:
            # The code to run subtask exists here. If the code block runs succesfully, set succesfull_run = True
            if not successfull_run:
                successfull_run = self.run_sub_task(state, retries = retries - 1)
        return successfull_run
    
    def run_task(self):
        for state in self.state_list:
            query = "select * from task where country = ? and state = ?"
            params = (self.country, state)
            task_table_entry = sql.execute(query=query, params=params).fetchone()
            if task_table_entry is None:
                query = f"insert into task(idempotency_key, country, state, job_id, status_code) select {self.idempotency_key}, {self.id}, {self.country}, {state}, 'RUNNING'"
                sql.execute(query=query)
                sub_task_result_status = self.run_sub_task(state, retries=3)
                if sub_task_result_status:
                    continue
                else:
                    break
            else:
                status_code = task_table_entry.status_code
                if status_code == "SUCCESS":
                    pass
                elif status_code == "FAILURE":
                    self.run_sub_task(state=state)
                else:
                    break
        
        return
