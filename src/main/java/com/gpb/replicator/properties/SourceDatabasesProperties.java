package com.gpb.replicator.properties;

import com.gpb.replicator.dto.SourceDbConnections;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "replication.databases")
public class SourceDatabasesProperties {
    private List<SourceDbConnections> info = new ArrayList<>();

}
