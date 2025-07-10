package com.flinkreplicationservice.service;

import com.flinkreplicationservice.flink.MetadataExtractorByDatabase;
import com.flinkreplicationservice.properties.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReplicationService {

    private final ExecutionEnvironment env;
    private final TargetDateBaseProperty targetDateBaseProperty;
    private final SourceTablesProperties sourceTablesProperties;
    private final SourceDatabasesProperties sourceDatabasesProperties;
    private final FlinkProperty flinkProperty;

    public void startReplication() throws Exception {
        Instant startTime = Instant.now();
        log.info("▶️ Репликация метаданных начата: {}", startTime);

        env.setParallelism(flinkProperty.getParallelism() > 0 ? flinkProperty.getParallelism() : 5);

        List<String> tablesToReplicate = sourceTablesProperties.getTables();
        List<SourceDbProperties> activeSources = sourceDatabasesProperties.getInfo().stream()
                .filter(SourceDbProperties::isActive)
                .toList();

        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                TypeInformation.of(String.class),     // data_source
                TypeInformation.of(String.class),     // table_name
                TypeInformation.of(String.class),     // data (json)
                TypeInformation.of(Timestamp.class)   // updated_at
        );

        var rowsDS = env.fromCollection(activeSources)
                .flatMap(new MetadataExtractorByDatabase(tablesToReplicate, flinkProperty.getMaxRetries(), flinkProperty.getRetryDelayMs()))
                .returns(rowTypeInfo);

        rowsDS.output(
                JDBCOutputFormat.buildJDBCOutputFormat()
                        .setDrivername("org.postgresql.Driver")
                        .setDBUrl(targetDateBaseProperty.getUrl())
                        .setUsername(targetDateBaseProperty.getUsername())
                        .setPassword(targetDateBaseProperty.getPassword())
                        .setQuery("INSERT INTO metadata (data_source, table_name, data, updated_at) VALUES (?, ?, ?::jsonb, ?)")
                        .setSqlTypes(new int[]{
                                Types.VARCHAR,   // источник бд
                                Types.VARCHAR,   // имя системной таблицы
                                Types.VARCHAR,   // строка из таблицы в виде json
                                Types.TIMESTAMP  // дата последнего обновления
                        })
                        .finish()
        );

        env.execute("Replicate metadata to unified metadata table");

        Instant endTime = Instant.now();
        Duration duration = Duration.between(startTime, endTime);

        log.info("✅ Репликация метаданных завершена: {}", endTime);
        log.info("⏱ Общее время выполнения: {} секунд", duration.toSeconds());
    }
}
