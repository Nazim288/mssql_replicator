package com.gpb.replicator.service;

import com.gpb.replicator.dto.ReplicationRequestDto;

public interface ReplicationJobService {
    void addToQueue(ReplicationRequestDto dto);
}
