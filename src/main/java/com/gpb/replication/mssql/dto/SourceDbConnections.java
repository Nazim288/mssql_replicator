package com.gpb.replication.mssql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class SourceDbConnections implements Serializable {
    private String name;
    @JsonProperty("service_name")
    private String serviceName;
    @JsonProperty("db_type")
    private String dbType;
    private String url;
    private String username;
    private String password;
    private boolean active;
    private String schema;
    private int priority;
}
