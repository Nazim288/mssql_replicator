package com.gpb.replication.mssql.logrepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.gpb.replication.mssql.properties.CleanDatabaseLogsProperties;
import com.gpb.replication.mssql.properties.LogsDatabaseProperties;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Slf4j
@Repository
@RequiredArgsConstructor
public class LogPartitionRepository {

    private final JdbcTemplate logsJdbcTemplate;
    private final CleanDatabaseLogsProperties cleanDatabaseLogs;
    private final LogsDatabaseProperties logsDatabaseProperties;

    public void createTodayPartition() {
        if (!logsDatabaseProperties.isEnabled()) {
            return;
        }

        LocalDate currentDate = LocalDate.now();
        String partitionName = String.format("postgres_replicator_log_%s",
                currentDate.format(DateTimeFormatter.ofPattern("yyyy_MM_dd")));

        String sql = String.format("""
                CREATE TABLE IF NOT EXISTS %s 
                PARTITION OF postgres_replicator_log 
                FOR VALUES FROM ('%s 00:00:00') TO ('%s 00:00:00')
                """,
                partitionName,
                currentDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                currentDate.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
        );

        logsJdbcTemplate.execute(sql);
    }

    public void dropOldPartitions() {
        if (!logsDatabaseProperties.isEnabled()) {
            return;
        }

        LocalDate dateToDelete = LocalDate.now().minusYears(cleanDatabaseLogs.getCleanPeriod());
        String partitionName = String.format("postgres_replicator_log_%d_%02d_%02d",
                dateToDelete.getYear(),
                dateToDelete.getMonthValue(),
                dateToDelete.getDayOfMonth()
        );

        String sql = String.format("DROP TABLE IF EXISTS %s", partitionName);

        logsJdbcTemplate.execute(sql);
    }
}