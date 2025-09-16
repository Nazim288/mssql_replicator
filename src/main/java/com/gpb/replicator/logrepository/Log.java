package com.gpb.replicator.logrepository;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;


//@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
//@Table(name = "postgres_replicator_log")
public class Log {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;
    private String log;
    private String type;

    public Log(Date created, String log, String type) {
        this.created = created;
        this.log = log;
        this.type = type;
    }
}
