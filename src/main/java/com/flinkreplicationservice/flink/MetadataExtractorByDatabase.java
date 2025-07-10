package com.flinkreplicationservice.flink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.flinkreplicationservice.properties.SourceDbProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class MetadataExtractorByDatabase implements FlatMapFunction<SourceDbProperties, Row> {

    private static final Logger log = LoggerFactory.getLogger(MetadataExtractorByDatabase.class);

    private static final int ROW_FIELD_COUNT = 4;
    private static final int IDX_SOURCE_NAME = 0;
    private static final int IDX_TABLE_NAME = 1;
    private static final int IDX_DATA_JSON = 2;
    private static final int IDX_TIMESTAMP = 3;

    private final List<String> tables;
    private final int maxRetries;
    private final long retryDelayMs;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void flatMap(SourceDbProperties source, Collector<Row> collector) {
        Connection conn = null;
        Timestamp currentTimestamp = Timestamp.valueOf(LocalDateTime.now()); // создаём один раз

        try {
            conn = connectWithRetries(source);
            for (String tableName : tables) {
                processTable(conn, source, tableName, collector, currentTimestamp);
            }
        } catch (Exception e) {
            log.error("Failed to extract metadata for DB {}: {}", source.getName(), e.getMessage(), e);
        } finally {
            closeQuietly(conn);
        }
    }

    private Connection connectWithRetries(SourceDbProperties source) throws InterruptedException {
        int attempt = 0;
        while (attempt < maxRetries) {
            try {
                return DriverManager.getConnection(source.getUrl(), source.getUsername(), source.getPassword());
            } catch (SQLException e) {
                attempt++;
                log.warn("Connection attempt {}/{} failed for DB {}: {}", attempt, maxRetries, source.getName(), e.getMessage());
                if (attempt >= maxRetries) {
                    throw new RuntimeException("Max retries reached for DB " + source.getName(), e);
                }
                Thread.sleep(retryDelayMs);
            }
        }
        throw new RuntimeException("Unexpected failure connecting to DB " + source.getName());
    }

    private void processTable(Connection conn, SourceDbProperties source, String tableName, Collector<Row> collector, Timestamp currentTimestamp) {
        String sql = "SELECT * FROM " + tableName;
        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            while (rs.next()) {
                Map<String, Object> rowMap = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = meta.getColumnName(i);
                    Object value = rs.getObject(i);
                    value = convertJdbcValue(value);
                    rowMap.put(columnName, value);
                }

                Row flinkRow = new Row(ROW_FIELD_COUNT);
                flinkRow.setField(IDX_SOURCE_NAME, source.getName());
                flinkRow.setField(IDX_TABLE_NAME, tableName);
                flinkRow.setField(IDX_DATA_JSON, objectMapper.writeValueAsString(rowMap));
                flinkRow.setField(IDX_TIMESTAMP, Timestamp.valueOf(LocalDateTime.now()));

                collector.collect(flinkRow);
            }

        } catch (SQLException | JsonProcessingException e) {
            log.error("Error querying table {} from DB {}: {}", tableName, source.getName(), e.getMessage(), e);
        }
    }

    private Object convertJdbcValue(Object value) {
        if (value instanceof Array array) {
            try {
                Object[] arr = (Object[]) array.getArray();
                return Arrays.asList(arr);
            } catch (SQLException e) {
                log.warn("Failed to read SQL Array: {}", e.getMessage());
                return "[UNREADABLE ARRAY]";
            }
        } else if (value instanceof Clob clob) {
            try {
                return clob.getSubString(1, (int) clob.length());
            } catch (SQLException e) {
                log.warn("Failed to read CLOB: {}", e.getMessage());
                return "[UNREADABLE CLOB]";
            }
        } else if (value instanceof Blob blob) {
            try {
                return "[BLOB " + blob.length() + " bytes]";
            } catch (SQLException e) {
                return "[UNREADABLE BLOB]";
            }
        } else if (value instanceof Struct struct) {
            return struct.toString();
        } else if (value != null && value.getClass().getName().startsWith("org.postgresql")) {
            return value.toString();
        }
        return value;
    }

    private void closeQuietly(AutoCloseable ac) {
        if (ac != null) {
            try {
                ac.close();
            } catch (Exception e) {
                log.warn("Error closing resource: {}", e.getMessage());
            }
        }
    }
}