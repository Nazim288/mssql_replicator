package com.gpb.replicator.flink;

import com.gpb.replicator.dto.SourceDbConnections;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.DigestUtils;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

@RequiredArgsConstructor
public class MetadataExtractorByDatabase implements FlatMapFunction<SourceDbConnections, Row> {

    private static final Logger log = LoggerFactory.getLogger(MetadataExtractorByDatabase.class);

    private static final int ROW_FIELD_COUNT = 9;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final int maxRetries;
    private final long retryDelayMs;

    @Override
    public void flatMap(SourceDbConnections source, Collector<Row> collector) {
        Timestamp currentTimestamp = Timestamp.valueOf(LocalDateTime.now());
        List<String> userDatabases = getAllUserDatabases(source);

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("SQL Server JDBC Driver not found", e);
        }

        for (String dbName : userDatabases) {
            // String dbUrl = buildDbUrl(source.getUrl(), dbName);
            try (Connection conn = connectWithRetries(source.getUrl(), source.getUsername(), source.getPassword())) {
                List<String> userTables = getAllUserTables(conn);

                for (String tableFullName : userTables) {
                    String[] parts = tableFullName.split("\\.");
                    String schema = parts[0];
                    String tableName = parts[1];

                    processTableColumns(conn, source, schema, tableName, collector, currentTimestamp, dbName);
                }

            } catch (SQLException | InterruptedException e) {
                log.error("Не удалось подключиться к базе {}: {}", dbName, e.getMessage(), e);
            }
        }
    }

    private Connection connectWithRetries(String url, String username, String password) throws InterruptedException {
        int attempt = 0;
        while (attempt < maxRetries) {
            try {
                return DriverManager.getConnection(url, username, password);
            } catch (SQLException e) {
                attempt++;
                log.warn("Попытка подключения {}/{} не удалась для URL {}: {}", attempt, maxRetries, url, e.getMessage());
                if (attempt >= maxRetries) throw new RuntimeException("Достигнут предел попыток подключения к БД " + url, e);
                Thread.sleep(retryDelayMs);
            }
        }
        throw new RuntimeException("Неожиданная ошибка подключения к БД " + url);
    }

    private List<String> getAllUserDatabases(SourceDbConnections source) {
        List<String> databases = new ArrayList<>();
        // String adminUrl = buildAdminUrl(source.getUrl());

        String sql = "SELECT name as datname \n" + //
                    "FROM sys.databases \n" + //
                    "WHERE source_database_id IS NULL  -- не является snapshot\n" + //
                    "  AND is_read_only = 0           -- не только для чтения\n" + //
                    "  AND state = 0                  -- ONLINE (доступна)\n" + //
                    "  AND name NOT IN ('master', 'tempdb', 'model', 'msdb');";

        try (Connection conn = DriverManager.getConnection(source.getUrl(), source.getUsername(), source.getPassword());
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                databases.add(rs.getString("datname"));
            }

        } catch (SQLException e) {
            log.error("Ошибка при получении списка баз для {}: {}", source.getName(), e.getMessage(), e);
        }

        return databases;
    }

    private List<String> getAllUserTables(Connection conn) throws SQLException {
        List<String> userTables = new ArrayList<>();
        String sql = """
                SELECT 
                    s.name AS schema_name,
                    t.name AS table_name,
                    t.type_desc AS table_type
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
                WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
                AND (t.is_ms_shipped = 0 OR t.name LIKE 'sys%') -- исключаем системные таблицы
                AND (t.is_filetable = 0) -- исключаем FileTables
                ORDER BY s.name, t.name;       
                """;

        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                String schema = rs.getString("schema_name");
                String table = rs.getString("table_name");
                userTables.add(schema + "." + table);
            }
        }

        return userTables;
    }

    private void processTableColumns(Connection conn, SourceDbConnections source, String schema, String tableName,
                                     Collector<Row> collector, Timestamp currentTimestamp, String dbName) {
        String sql = """
            SELECT 
                c.name AS column_name,
                t.name AS data_type,
                c.max_length,
                c.precision,
                c.scale,
                CASE WHEN c.is_nullable = 0 THEN 1 ELSE 0 END AS not_null,
                OBJECT_DEFINITION(c.default_object_id) AS column_default,
                c.column_id AS ordinal_position,
                CASE WHEN ic.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key
            FROM sys.columns c
            INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
            INNER JOIN sys.tables tab ON c.object_id = tab.object_id
            INNER JOIN sys.schemas s ON tab.schema_id = s.schema_id
            LEFT JOIN sys.index_columns ic ON ic.object_id = c.object_id 
                AND ic.column_id = c.column_id
            LEFT JOIN sys.indexes i ON i.object_id = ic.object_id 
                AND i.index_id = ic.index_id AND i.is_primary_key = 1
            WHERE tab.name = ?  -- имя таблицы
                AND s.name = ?  -- имя схемы
                AND c.is_computed = 0  -- исключаем вычисляемые столбцы
            ORDER BY c.column_id;
        """;

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, tableName);
            stmt.setString(2, schema);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> rowMap = new LinkedHashMap<>();
                    rowMap.put("column_name", rs.getString("column_name"));
                    rowMap.put("data_type", rs.getString("data_type"));
                    rowMap.put("not_null", rs.getBoolean("not_null"));
                    rowMap.put("column_default", rs.getString("column_default"));
                    rowMap.put("column_position", rs.getInt("ordinal_position"));

                    rowMap.put("data_source", source.getName());
                    rowMap.put("database", dbName);
                    rowMap.put("schema", schema);
                    rowMap.put("table", tableName);

                    String jsonData = objectMapper.writeValueAsString(rowMap);
                    String recordKey = buildRecordKey(rowMap);
                    String dataHash = DigestUtils.md5DigestAsHex(jsonData.getBytes(StandardCharsets.UTF_8));

                    Row flinkRow = new Row(ROW_FIELD_COUNT);
                    flinkRow.setField(0, source.getName());   // data_source
                    flinkRow.setField(1, tableName);          // table_name
                    flinkRow.setField(2, schema);             // schema_name
                    flinkRow.setField(3, recordKey);          // record_key
                    flinkRow.setField(4, dataHash);           // data_hash
                    flinkRow.setField(5, jsonData);           // data
                    flinkRow.setField(6, currentTimestamp);   // updated_at
                    flinkRow.setField(7, source.getDbType()); // db_type
                    flinkRow.setField(8, dbName);             // db_name


                    collector.collect(flinkRow);
                }
            }

        } catch (SQLException | JsonProcessingException e) {
            log.error("Ошибка при обработке таблицы {}.{} из базы {}: {}", schema, tableName, dbName, e.getMessage(), e);
        }
    }

    private String buildRecordKey(Map<String, Object> rowMap) {
        return String.join("|",
                safe(rowMap.get("data_source")),
                safe(rowMap.get("database")),
                safe(rowMap.get("schema")),
                safe(rowMap.get("table")),
                safe(rowMap.get("column_name"))
        );
    }

    private String safe(Object value) {
        return value != null ? value.toString() : "NULL";
    }

    // private String buildAdminUrl(String originalUrl) {
    //     log.info("Original url: {}", originalUrl);
    //     String url = originalUrl.trim();
    //     if (!url.matches(".*/[^/]+$")) {
    //         if (!url.endsWith("/")) url += "/";
    //         url += "mssql";
    //     } else {
    //         url = url.replaceFirst("/[^/]+$", "/mssql");
    //     }
    //     return url;
    // }

    // private String buildDbUrl(String originalUrl, String dbName) {
    //     String url = originalUrl.trim();
    //     if (!url.matches(".*/[^/]+$")) {
    //         if (!url.endsWith("/")) url += "/";
    //         url += dbName;
    //     } else {
    //         url = url.replaceFirst("/[^/]+$", "/" + dbName);
    //     }
    //     return url;
    // }
}
