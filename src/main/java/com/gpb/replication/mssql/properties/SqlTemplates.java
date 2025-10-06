package com.gpb.replication.mssql.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component("sqlTemplates")
public class SqlTemplates {

    private final String databaseSql = """
            SELECT database_id AS oid, name AS datname
            FROM sys.databases
            WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
              AND state = 0 -- ONLINE
              AND is_read_only = 0;
            """;

    private final String schemaSql = """
            SELECT schema_id AS oid, name AS schema_name
            FROM sys.schemas
            WHERE name NOT IN (
                'information_schema','sys','db_owner','db_accessadmin','db_securityadmin',
                'db_ddladmin','db_backupoperator','db_datareader','db_datawriter',
                'db_denydatareader','db_denydatawriter','guest'
            );
            """;

    private final String tableSql = """
    SELECT 
        t.object_id AS oid,
        s.name AS schema_name,
        t.name AS table_name,
        CAST(ep.value AS NVARCHAR(MAX)) AS description,
        (
            SELECT
                'REGULAR' AS tableType,
                sm.definition AS viewDefinition,
                (
                    SELECT 
                        c.name AS [name],
                        CONCAT(DB_NAME(), '.', s.name, '.', t.name, '.', c.name) AS [fqn],
                        ty.name AS [dataType],
                        ty.name +
                          CASE 
                              WHEN ty.name IN ('varchar','nvarchar','char','nchar') 
                                   THEN '(' + IIF(c.max_length = -1, 'MAX', CAST(c.max_length AS VARCHAR)) + ')'
                              WHEN ty.name IN ('decimal','numeric') 
                                   THEN '(' + CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR) + ')'
                              ELSE ''
                          END AS [dataTypeDisplay],
                        c.max_length AS [dataLength],
                        ISNULL(CAST(epc.value AS NVARCHAR(MAX)), '') AS [description],
                        c.column_id AS [ordinalPosition],
                        IIF(c.is_nullable = 0, 'NOT_NULL', 'NULLABLE') AS [constraint]
                    FROM sys.columns c
                    INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
                    LEFT JOIN sys.extended_properties epc 
                        ON epc.major_id = c.object_id 
                        AND epc.minor_id = c.column_id 
                        AND epc.class = 1
                    WHERE c.object_id = t.object_id
                    FOR JSON PATH
                ) AS columns,
                (
                     SELECT
                         JSON_QUERY(
                             '[' + STRING_AGG(QUOTENAME(kcu.COLUMN_NAME, '"'), ',') + ']'
                         ) AS columns,
                         CASE
                             WHEN tc.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 'PRIMARY_KEY'
                             WHEN tc.CONSTRAINT_TYPE = 'FOREIGN KEY' THEN 'FOREIGN_KEY'
                             WHEN tc.CONSTRAINT_TYPE = 'UNIQUE' THEN 'UNIQUE'
                             WHEN tc.CONSTRAINT_TYPE = 'CHECK' THEN 'CHECK'
                             ELSE tc.CONSTRAINT_TYPE
                         END AS constraintType
                     FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                     JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                       ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                      AND tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
                     WHERE tc.TABLE_NAME = t.name
                       AND tc.TABLE_SCHEMA = s.name
                     GROUP BY tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE
                     FOR JSON PATH
                 ) AS tableConstraints
                 ,
                t.type AS rawTableType
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ) AS table_structure
    FROM sys.objects t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    LEFT JOIN sys.extended_properties ep 
        ON ep.major_id = t.object_id AND ep.minor_id = 0 AND ep.class = 1
    LEFT JOIN sys.sql_modules sm 
        ON sm.object_id = t.object_id
    WHERE s.name NOT IN ('information_schema','sys')
      AND t.type IN ('U', 'V'); -- включаем таблицы и вьюхи
    """;

}

