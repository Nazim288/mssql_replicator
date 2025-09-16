package com.gpb.replicator.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "replication")
public class ReplicationProperties {
    private boolean enabled;
    private int maxRetries;

}