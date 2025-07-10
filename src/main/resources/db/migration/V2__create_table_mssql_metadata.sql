CREATE TABLE IF NOT EXISTS mssql_metadata.metadata (
                                                       id SERIAL PRIMARY KEY,
                                                       data_source VARCHAR NOT NULL,
                                                       table_name VARCHAR NOT NULL,
                                                       data JSONB NOT NULL,
                                                       updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
    );