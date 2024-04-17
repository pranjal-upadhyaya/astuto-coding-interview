-- DROP TABLE IF EXISTS job;
-- DROP TABLE IF EXISTS task;

create table job (
    id INTEGER primary key autoincrement, 
    idempotency_key INTEGER UNIQUE not null  
);

create table task (
    idempotency_key INTEGER not null,
    country TEXT not null,
    state TEXT not null,
    status_code TEXT not null,
    job_id INTEGER not null,
    FOREIGN KEY (job_id) REFERENCES job (id)
);
