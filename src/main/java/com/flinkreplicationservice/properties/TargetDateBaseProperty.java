package com.flinkreplicationservice.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "replication.mssql.target-database-property")
public class TargetDateBaseProperty {
    private String url;
    private String username;
    private String password;
}
