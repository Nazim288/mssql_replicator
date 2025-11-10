package com.gpb.replication.mssql.logrepository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.gpb.replication.mssql.properties.CleanDatabaseLogsProperties;
import com.gpb.replication.mssql.properties.LogsDatabaseProperties;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Slf4j
@Repository
public class LogPartitionRepository {

    private final JdbcTemplate logsJdbcTemplate;
    private final CleanDatabaseLogsProperties cleanDatabaseLogs;
    private final LogsDatabaseProperties logsDatabaseProperties;

    public LogPartitionRepository(@Qualifier("logsJdbcTemplate") JdbcTemplate logsJdbcTemplate, CleanDatabaseLogsProperties cleanDatabaseLogs,
                                  LogsDatabaseProperties logsDatabaseProperties
    ) {
        this.logsJdbcTemplate = logsJdbcTemplate;
        this.cleanDatabaseLogs = cleanDatabaseLogs;
        this.logsDatabaseProperties = logsDatabaseProperties;
    }

    public void createTodayPartition() {
        if (!logsDatabaseProperties.isEnabled()) {
            return;
        }

        String table = logsDatabaseProperties.getTable().trim();
        LocalDate currentDate = LocalDate.now();
        String partitionName = String.format("%s_%s",
                table,
                currentDate.format(DateTimeFormatter.ofPattern("yyyy_MM_dd")));

        String sql = String.format("""
                        CREATE TABLE IF NOT EXISTS %s
                        PARTITION OF %s
                        FOR VALUES FROM ('%s 00:00:00') TO ('%s 00:00:00')
                        """,
                partitionName,
                table,
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
        String partitionName = String.format(logsDatabaseProperties.getTable().trim() + "_%d_%02d_%02d",
                dateToDelete.getYear(),
                dateToDelete.getMonthValue(),
                dateToDelete.getDayOfMonth()
        );

        String sql = String.format("DROP TABLE IF EXISTS %s", partitionName);

        logsJdbcTemplate.execute(sql);
    }
}