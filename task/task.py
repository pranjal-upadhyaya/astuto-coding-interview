import sqlite3
import time

class Task:

    def __init__(self, job_id, key, input: dict):
        country = input["country"]
        state_list = input["states"]
        self.job_id = job_id
        self.idempotency_key = key
        self.country = country
        self.state_list = state_list
        self.run_task()

    def run_sub_task(self, state: str, retries = 3):
        successfull_run = False
        if retries > 0:
            # The code to run subtask exists here. If the code block runs succesfully, set succesfull_run = True
            try:
                time.sleep(5)
                print("state")
                successfull_run = True
            except Exception as e:
                pass
            if not successfull_run:
                successfull_run = self.run_sub_task(state, retries = retries - 1)
        # print(successfull_run)
        return successfull_run
    
    def run_task(self):
        for state in self.state_list:
            print(state)
            con = sqlite3.connect("task.db")
            cur = con.cursor()
            query = "select * from task where country = ? and state = ?"
            params = (self.country, state, )
            task_table_entry = cur.execute(query, params, ).fetchone()
            if task_table_entry is None:
                query = "insert into task(idempotency_key, country, state, job_id, status_code) values (?, ?, ?, ?, ?)"
                params = (self.idempotency_key, self.country, state, self.job_id, "RUNNING", )
                cur.execute(query, params)
                con.commit()
                sub_task_result_status = self.run_sub_task(state, retries=3)
                print(sub_task_result_status)
                if sub_task_result_status:
                    query = "update task set status_code = 'SUCCESS' where country = ? and state = ?"
                    params = (self.country, state)
                    cur.execute(query, params, )
                    con.commit()
                else:
                    query = "update task set status_code = 'FAILURE' where country = ? and state = ?"
                    params = (self.country, state)
                    cur.execute(query, params, )
                    con.commit()
                    break
            else:
                status_code = task_table_entry[3]
                if status_code == "SUCCESS":
                    continue
                elif status_code == "FAILURE":
                    sub_task_result_status = self.run_sub_task(state, retries=3)
                    if sub_task_result_status:
                        query = "update task set status_code = 'SUCCESS' where country = ? and state = ?"
                        params = (self.country, state)
                        cur.execute(query, params, )
                        con.commit()
                    else:
                        query = "update task set status_code = 'FAILURE' where country = ? and state = ?"
                        params = (self.country, state)
                        cur.execute(query, params, )
                        con.commit()
                        break
                else:
                    break
        
        return
