package com.gpb.replication.mssql.service;

public interface ReplicationService {
   void startReplicationAsync(String serviceName);
   void startReplication(String serviceName);
}
