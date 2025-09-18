package com.gpb.replication.mssql.properties;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
@Configuration
@ConfigurationProperties(prefix = "logs-database")
@RequiredArgsConstructor
@Data
public class LogsDatabaseProperties {
    private boolean enabled;
    private String url;
    private String username;
    private String password;
    private String driverClassName;
}
