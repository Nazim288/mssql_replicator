package com.gpb.replicator.service.impl;

import com.gpb.replicator.enums.ReplicationJobStatus;
import com.gpb.replicator.dto.SourceDbConnections;
import com.gpb.replicator.flink.MetadataExtractorByDatabase;
import com.gpb.replicator.log.SvoiCustomLogger;
import com.gpb.replicator.log.SvoiSeverityEnum;
import com.gpb.replicator.model.ReplicationJob;
import com.gpb.replicator.properties.*;
import com.gpb.replicator.repository.ReplicationJobRepository;
import com.gpb.replicator.service.DbSourcesService;
import com.gpb.replicator.service.ReplicationService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReplicationServiceImpl implements ReplicationService {

    private final ExecutionEnvironment env;
    private final DataSourceProperties dataSourceProperties;
    private final MssqlTablesProperties mssqlProperties;
    private final FlinkProperty flinkProperty;
    private final ReplicationJobRepository jobRepository;
    private final DbSourcesService dbSourcesService;
    private final SvoiCustomLogger svoiCustomLogger;

    @Scheduled(fixedDelay = 60000)
    @Transactional
    public void scheduledReplication() {
        log.info("Запуск репликации...");
        try {
            startReplication();
        } catch (Exception e) {
            log.error("Ошибка при выполнении репликации", e);
        }
    }

    public void startReplication() {
        env.setParallelism(flinkProperty.getParallelism() > 0 ? flinkProperty.getParallelism() : 1);

        List<SourceDbConnections> sourceDbCForReplication = new ArrayList<>();
        List<SourceDbConnections> sourceDbNotFound = new ArrayList<>();

        Map<String, SourceDbConnections> dbConnectionsMap = dbSourcesService.getDbConnections()
                .stream()
                .collect(Collectors.toMap(
                        SourceDbConnections::getName,
                        Function.identity()
                ));

        List<ReplicationJob> pendingJobs = jobRepository.findByStatusOrderByCreatedAt(ReplicationJobStatus.PENDING);

        List<ReplicationJob> runningJobs = new ArrayList<>();

        pendingJobs.forEach(job -> {
            if (dbConnectionsMap.containsKey(job.getDbName())) {
                sourceDbCForReplication.add(dbConnectionsMap.get(job.getDbName()));
                job.setStatus(ReplicationJobStatus.RUNNING);
                runningJobs.add(job);
            } else {
                sourceDbNotFound.add(dbConnectionsMap.get(job.getDbName()));
                job.setStatus(ReplicationJobStatus.FAILED);
            }
        });

        List<String> tablesToReplicate = mssqlProperties.getTables();

        if (!sourceDbCForReplication.isEmpty()) {
            log.info("Полученно из очереди {} источников", sourceDbCForReplication.size());
        }
        if (!sourceDbNotFound.isEmpty()) {
            log.info("Не найденно данных по {} источникам для репликации", sourceDbNotFound.size());
        }
        if (tablesToReplicate.isEmpty()) {
            log.info("Не получен список системных таблиц");
        }

        jobRepository.saveAll(pendingJobs);

        // теперь у нас 6 колонок: data_source, table_name, record_key, data_hash, data, updated_at
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                TypeInformation.of(String.class),    // data_source
                TypeInformation.of(String.class),    // table_name (только имя таблицы)
                TypeInformation.of(String.class),    // schema_name (новая колонка)
                TypeInformation.of(String.class),    // record_key
                TypeInformation.of(String.class),    // data_hash
                TypeInformation.of(String.class),    // data (json)
                TypeInformation.of(Timestamp.class), // updated_at
                TypeInformation.of(String.class),    // db_type
                TypeInformation.of(String.class)     // db_name
        );


        if (!sourceDbCForReplication.isEmpty()) {
            run(rowTypeInfo, sourceDbCForReplication);

            runningJobs.forEach(job -> {
                job.setStatus(ReplicationJobStatus.DONE);
            });
            jobRepository.saveAll(runningJobs);

            log.info("Репликация завершена успешно для источников : {}",
                    sourceDbCForReplication.stream()
                            .map(SourceDbConnections::getName)
                            .collect(Collectors.joining(", "))
            );

            String replicatedSources = sourceDbCForReplication.stream()
                    .map(SourceDbConnections::getName)
                    .collect(Collectors.joining(", "));

            String notFoundSources = sourceDbNotFound.stream()
                    .map(SourceDbConnections::getName)
                    .collect(Collectors.joining(", "));

            svoiCustomLogger.send(
                    "replicationJob",
                    "Replication Finished",
                    String.format("Replicated sources: [%s]; Not found: [%s]; Tables: [%s]",
                            replicatedSources,
                            notFoundSources.isEmpty() ? "none" : notFoundSources,
                            String.join(", ", tablesToReplicate)),
                    SvoiSeverityEnum.ONE
            );
        }
    }

    public void run(RowTypeInfo rowTypeInfo, List<SourceDbConnections> activeSources) {

        var rowsDS = env.fromCollection(activeSources)
                .flatMap(new MetadataExtractorByDatabase(flinkProperty.getMaxRetries(), flinkProperty.getRetryDelayMs()))
                .returns(rowTypeInfo);

        rowsDS.output(
                JDBCOutputFormat.buildJDBCOutputFormat()
                        .setDrivername("org.postgresql.Driver")
                        .setDBUrl(dataSourceProperties.getUrl())
                        .setUsername(dataSourceProperties.getUsername())
                        .setPassword(dataSourceProperties.getPassword())
                        .setQuery(
                                "INSERT INTO metadata " +
                                        "    (data_source, table_name, schema_name, record_key, data_hash, data, updated_at, db_type, db_name) " +
                                        "VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?) " +
                                        "ON CONFLICT (data_source, table_name, schema_name, record_key) DO UPDATE SET " +
                                        "    data_hash  = EXCLUDED.data_hash, " +
                                        "    data       = EXCLUDED.data, " +
                                        "    updated_at = EXCLUDED.updated_at, " +
                                        "    db_type    = EXCLUDED.db_type, " +
                                        "    db_name    = EXCLUDED.db_name " +
                                        "WHERE metadata.data_hash IS DISTINCT FROM EXCLUDED.data_hash"
                        )
                        .setSqlTypes(new int[]{
                                Types.VARCHAR,   // data_source
                                Types.VARCHAR,   // table_name
                                Types.VARCHAR,   // schema_name (новая колонка)
                                Types.VARCHAR,   // record_key
                                Types.VARCHAR,   // data_hash
                                Types.VARCHAR,   // data (JSONB)
                                Types.TIMESTAMP, // updated_at
                                Types.VARCHAR,   // db_type
                                Types.VARCHAR    // db_name
                        })
                        .finish()
        );

        try {
            env.execute("Репликация метаданных в единую таблицу метаданных");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
