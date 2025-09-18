package com.gpb.replication.mssql.service;


import java.util.List;

import com.gpb.replication.mssql.dto.SourceDbConnections;

public interface DbSourcesService {
     List<SourceDbConnections> getDbConnections();
}
