package com.gpb.replicator.service.impl;


import com.gpb.replicator.enums.ReplicationJobStatus;
import com.gpb.replicator.dto.ReplicationRequestDto;
import com.gpb.replicator.model.ReplicationJob;
import com.gpb.replicator.repository.ReplicationJobRepository;
import com.gpb.replicator.service.ReplicationJobService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ReplicationJobServiceImpl implements ReplicationJobService {
    private final ReplicationJobRepository replicationJobRepository;

    @Transactional
    public void addToQueue(ReplicationRequestDto dto) {
        String dbName = dto.getDbName();
        if (dbName == null || dbName.isBlank()) {
            throw new IllegalArgumentException("Database name must be provided");
        }

        ReplicationJob job = ReplicationJob.builder()
                .dbName(dbName)
                .status(ReplicationJobStatus.PENDING)
                .build();

        replicationJobRepository.save(job);
    }
}
