package com.gpb.replicator.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;


@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "replication.mssql")
public class MssqlTablesProperties {

    private List<String> tables;
}
