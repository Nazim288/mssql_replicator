CREATE TABLE IF NOT EXISTS mssql_metadata.metadata (
                                                          id SERIAL PRIMARY KEY,
                                                          data_source VARCHAR NOT NULL,
                                                          table_name VARCHAR NOT NULL,
                                                          schema_name VARCHAR NOT NULL,
                                                          data JSONB NOT NULL,
                                                          updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
                                                          record_key text NOT NULL,
                                                          data_hash  text NOT NULL,
                                                          db_type varchar(100) NOT NULL,
                                                          db_name varchar(100) NOT NULL
    );

CREATE UNIQUE INDEX ux_metadata_key
    ON metadata(data_source, table_name, schema_name, record_key);