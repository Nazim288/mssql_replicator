CREATE TABLE metadata (
                          id INT IDENTITY(1,1) PRIMARY KEY,
                          data_source NVARCHAR(255) NOT NULL,
                          table_name NVARCHAR(255) NOT NULL,
                          data NVARCHAR(MAX) NOT NULL,           -- JSON будет храниться как строка
                          updated_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
);