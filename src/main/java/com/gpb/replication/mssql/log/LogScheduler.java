package com.gpb.replication.mssql.log;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.gpb.replication.mssql.logrepository.LogPartitionRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class LogScheduler {
    private final SvoiCustomLogger svoiCustomLogger;
    private final LogPartitionRepository logPartitionRepository;

    @Scheduled(cron = "${logs-database.task-create-partition}")
    public void createPartition() {
        logPartitionRepository.createTodayPartition();
    }
    @Scheduled(cron = "${clean-database-logs.task-cleaner-schedule}")
    public void cleanPartition() {
        logPartitionRepository.dropOldPartitions();
        svoiCustomLogger.send("cleanLogs", "Clean Logs", "Cleaned old logs", SvoiSeverityEnum.ONE);
    }
}
