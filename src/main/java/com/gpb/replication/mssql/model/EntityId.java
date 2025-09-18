package com.gpb.replication.mssql.model;

import java.io.Serializable;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.Data;

@Data
@Embeddable
public class EntityId implements Serializable{
    @Column(name = "id")
    private Long id;

    @Column(name = "service_name")
    private String serviceName;

    public EntityId(Long id, String serviceName) {
        this.id = id;
        this.serviceName = serviceName;
    }

    public EntityId() {
    }
}
