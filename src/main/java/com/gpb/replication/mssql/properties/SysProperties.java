package com.gpb.replication.mssql.properties;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
@ConfigurationProperties(prefix = "spring.application")
@RequiredArgsConstructor
@Data
public class SysProperties {
    private String name;
    private String version;
    @Value("${server.port:9200}")
    private int dpt;
    private String dntdom;
    private String user;
    private String ip;
    private String host;

    @PostConstruct
    public void init() {
        if (this.user == null) {
            this.user = System.getProperty("user.name");
        }
        try {
            this.host = InetAddress.getLocalHost().getHostName();
            this.ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            this.host = InetAddress.getLoopbackAddress().getHostName();
            this.ip = InetAddress.getLoopbackAddress().getHostAddress();
        }
    }
}

