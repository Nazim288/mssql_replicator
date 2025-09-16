
CREATE TABLE mssql_metadata.replication_jobs (
                                  id BIGSERIAL PRIMARY KEY,
                                  db_name TEXT NOT NULL,
                                  status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING, RUNNING, DONE, FAILED
                                  created_at TIMESTAMP DEFAULT NOW(),
                                  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_replication_jobs_status ON replication_jobs(status);

