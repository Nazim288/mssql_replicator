package com.gpb.replicator.logrepository;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Date;


@Repository
@RequiredArgsConstructor
public class LogRepository{

    private final JdbcTemplate logsJdbcTemplate;

    public Log findLatestByType(String type, String host) {
        String sql = """
            SELECT l.id, l.type, l.log, l.created
            FROM mssql_replicator_log l
            WHERE l.type = ?
              AND l.log LIKE ?
            ORDER BY l.created DESC
            LIMIT 1
        """;

        return logsJdbcTemplate.query(sql, new Object[]{type, "%" + host + "%"}, rs -> {
            if (rs.next()) {
                Log log = new Log();
                log.setId(rs.getObject("id", Integer.class));
                log.setType(rs.getString("type"));
                log.setLog(rs.getString("log"));
                log.setCreated(new Date(rs.getTimestamp("created").getTime()));
                return log;
            }
            return null;
        });
    }

    public void save(Log log) {
        String sql = """
            INSERT INTO mssql_replicator_log (created, log, type)
            VALUES (?, ?, ?)
        """;

        logsJdbcTemplate.update(sql,
                new java.sql.Timestamp(log.getCreated().getTime()),
                log.getLog(),
                log.getType());
    }
}