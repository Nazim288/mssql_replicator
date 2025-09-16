package com.gpb.replicator.service;


import com.gpb.replicator.dto.SourceDbConnections;

import java.util.List;

public interface DbSourcesService {
     List<SourceDbConnections> getDbConnections();
}
