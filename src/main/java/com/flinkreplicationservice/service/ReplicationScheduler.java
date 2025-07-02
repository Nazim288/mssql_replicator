package com.flinkreplicationservice.service;

import com.flinkreplicationservice.properties.ReplicationProperties;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


@Component
@RequiredArgsConstructor
public class ReplicationScheduler {
    private static final Logger log = LoggerFactory.getLogger(ReplicationScheduler.class);

    private final ReplicationService replicationService;
    private final ReplicationProperties replicationProperties;

    private int retryCount = 0;

    @PostConstruct
    public void init() {
        try {
            replicationService.startReplication();
        } catch (Exception e) {
            log.error("Error during initial replication: {}", e.getMessage(), e);
        }
    }

    @Scheduled(cron = "${flink.job.cron}")
    public void scheduledReplication() {
        if (!replicationProperties.isEnabled()) {
            log.info("Replication is disabled.");
            return;
        }

        try {
            replicationService.startReplication();
            retryCount = 0;
        } catch (Exception e) {
            retryCount++;
            log.error("Replication failed (attempt {}/{}): {}", retryCount, replicationProperties.getMaxRetries(), e.getMessage());

            if (retryCount >= replicationProperties.getMaxRetries()) {
                log.error("Max retries reached. Replication stopped.");
            }
        }
    }
}
