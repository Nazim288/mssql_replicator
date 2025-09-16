package com.gpb.replicator.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "replication.postgres.target-date-base-property")
public class TargetDateBaseProperty {
    private String url;
    private String username;
    private String password;
}
