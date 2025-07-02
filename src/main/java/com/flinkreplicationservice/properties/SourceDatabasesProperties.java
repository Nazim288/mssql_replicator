package com.flinkreplicationservice.properties;

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
    private List<SourceDbProperties> info = new ArrayList<>();

}
