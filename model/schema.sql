create table job(
    id not null, 
    idempotency_key not null,  
)

create table task (
    idempotency_key not null
    country not null
    state not null
    status_code not null
    job_id references job [id]
)
