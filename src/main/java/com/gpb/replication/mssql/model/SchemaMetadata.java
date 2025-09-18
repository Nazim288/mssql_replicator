package com.gpb.replication.mssql.model;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Data
@Table(name = "schema_metadata", schema = "mssql_metadata")
@EntityListeners(AuditingEntityListener.class)
public class SchemaMetadata {
    @EmbeddedId
    private EntityId id;
    
    @Column(name = "fqn")
    private String fqn;

    @Column(name = "db_name")
    private String dbName;

    @Column(name = "name")
    private String name;

    @Column(name = "parent_fqn")
    private String parentFqn;

    @Column(name = "hash_data")
    private String hashData;

    @CreatedDate
    @Column(name = "created_at", updatable = false)
    private Long createdAt;
}

