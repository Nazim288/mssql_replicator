CREATE TABLE mssql_metadata.metadata_rep (
                          id BIGSERIAL PRIMARY KEY,
                          data_source VARCHAR(255) NOT NULL,
                          database_name VARCHAR(255) NOT NULL,
                          schema_name VARCHAR(255) NOT NULL,
                          table_name VARCHAR(255) NOT NULL,
                          column_name VARCHAR(255),
                          data_type VARCHAR(100),
                          not_null BOOLEAN,
                          column_default TEXT,
                          constraint_types TEXT,
                          constraint_names TEXT,
                          data_hash VARCHAR(32),
                          data JSONB NOT NULL,
                          db_type VARCHAR(50),
                          service_name VARCHAR(255),
                          updated_at TIMESTAMP NOT NULL
);
