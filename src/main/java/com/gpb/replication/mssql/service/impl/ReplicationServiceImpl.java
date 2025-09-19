package com.gpb.replication.mssql.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.replication.mssql.dto.SourceDbConnections;
import com.gpb.replication.mssql.log.SvoiCustomLogger;
import com.gpb.replication.mssql.log.SvoiSeverityEnum;
import com.gpb.replication.mssql.model.DatabaseMetadata;
import com.gpb.replication.mssql.model.EntityId;
import com.gpb.replication.mssql.model.SchemaMetadata;
import com.gpb.replication.mssql.model.TableMetadata;
import com.gpb.replication.mssql.repository.DatabaseMetadataRepository;
import com.gpb.replication.mssql.repository.SchemaMetadataRepository;
import com.gpb.replication.mssql.repository.TableMetadataRepository;
import com.gpb.replication.mssql.service.DbSourcesService;
import com.gpb.replication.mssql.service.ReplicationService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.apache.commons.codec.digest.DigestUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReplicationServiceImpl implements ReplicationService {

    private final DbSourcesService dbSourcesService;
    private final SvoiCustomLogger svoiCustomLogger;

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final DatabaseMetadataRepository databaseRep;
    private final SchemaMetadataRepository schemaRep;
    private final TableMetadataRepository tableRep;

    @Async
    public void startReplicationAsync(String serviceName) {
        startReplication(serviceName);
    }

    public void startReplication(String serviceName) {
        // Чистим таблицы
        truncateTables(serviceName);

        Map<String, SourceDbConnections> dbConnectionsMap = dbSourcesService.getDbConnections()
                .stream()
                .collect(Collectors.toMap(
                        SourceDbConnections::getName,
                        Function.identity()
                ));

        if (dbConnectionsMap.containsKey(serviceName)) {
            SourceDbConnections source = dbConnectionsMap.get(serviceName);

            // Репликация баз данных
            List<String> databases = databaseReplication(source);

            
            for ( String dbName : databases ) {
                // Репликация схем
                schemaReplication(source, dbName);
                // Репликация таблиц
                tableReplication(source, dbName);
            }

            log.info("Репликация завершена успешно для источника {}", serviceName);
            svoiCustomLogger.send(
                    "replicationJob",
                    "Replication Finished",
                    String.format("Replicated source: [%s];",
                            serviceName),
                    SvoiSeverityEnum.ONE
            );

        } else {
            log.info("Не найденно данных по сервису {} для репликации", serviceName);
        }
    }

    private void truncateTables(String serviceName) {
        databaseRep.deleteByServiceName(serviceName);
        schemaRep.deleteByServiceName(serviceName);
        tableRep.deleteByServiceName(serviceName);
    }

    private List<String> databaseReplication(SourceDbConnections source) {
        String sql = """
            SELECT database_id AS oid, name AS datname 
            FROM sys.databases 
            WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
            AND state = 0 -- ONLINE
            AND is_read_only = 0;
        """;
        List<String> response = new ArrayList<>();
        LocalDateTime currentTime = LocalDateTime.now();

        try (Connection conn = DriverManager.getConnection(source.getUrl(), source.getUsername(), source.getPassword());
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            List<DatabaseMetadata> entities = new ArrayList<>();

            while (rs.next()) {
                DatabaseMetadata entity = new DatabaseMetadata();
                String fqn = getFqn(List.of(source.getServiceName(), rs.getString("datname")));

                EntityId id = new EntityId(rs.getLong("oid"),source.getServiceName());

                entity.setId(id);
                entity.setFqn(fqn);
                entity.setParentFqn(source.getServiceName());
                entity.setName(rs.getString("datname"));
                entity.setCreatedAt(currentTime);
                String hashString = fqn;

                // Подсчет хэш
                String hashData = DigestUtils.md5Hex(hashString);
                entity.setHashData(hashData);

                entities.add(entity);
                response.add(rs.getString("datname"));
            }
            databaseRep.saveAll(entities);

        } catch (SQLException e) {
            log.error("Ошибка при получении списка баз для {}: {}", source.getName(), e.getMessage(), e);
        }
        return response;
    }

    private void schemaReplication(SourceDbConnections source, String dbName) {
        String sql = """
            SELECT 
                schema_id AS oid, 
                name AS schema_name
            FROM sys.schemas
            WHERE name NOT IN ('information_schema', 'sys', 'db_owner', 'db_accessadmin', 
                            'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 
                            'db_datareader', 'db_datawriter', 'db_denydatareader', 
                            'db_denydatawriter', 'guest');
        """;
        List<SchemaMetadata> entities = new ArrayList<>();
        String url = buildDbUrl(source.getUrl(), dbName);
        LocalDateTime currentTime = LocalDateTime.now();

        try (Connection conn = DriverManager.getConnection(url, source.getUsername(), source.getPassword());
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                SchemaMetadata entity = new SchemaMetadata();
                String fqn = getFqn(List.of(source.getServiceName(), dbName, rs.getString("schema_name")));
                String parentFqn = fqn.substring(0, fqn.lastIndexOf("."));
                
                EntityId id = new EntityId(rs.getLong("oid"),source.getServiceName());

                entity.setId(id);
                entity.setFqn(fqn);
                entity.setDbName(dbName);
                entity.setName(rs.getString("schema_name"));
                entity.setParentFqn(parentFqn);
                entity.setCreatedAt(currentTime);

                // Подсчет хэш
                String hashString = fqn;
                String hashData = DigestUtils.md5Hex(hashString);
                entity.setHashData(hashData);

                entities.add(entity);
            }
            schemaRep.saveAll(entities);
        } catch (SQLException e) {
            log.error("Ошибка при получении схем для {}: {}", source.getName(), e.getMessage(), e);
        }
    }

    private void tableReplication(SourceDbConnections source, String dbName) {
        String sql = """
            SELECT 
                t.object_id AS oid,
                s.name AS schema_name,
                t.name AS table_name,
                CASE 
                    WHEN t.type = 'U' THEN 'regular'
                    WHEN t.type = 'V' THEN 'view'
                    ELSE 'other'
                END AS table_type,
                CAST(ep.value AS NVARCHAR(MAX)) AS description,
                (
                    SELECT 
                        JSON_QUERY(
                            (SELECT 
                                c.name AS [name],
                                CONCAT(DB_NAME(), '.', s.name, '.', t.name, '.', c.name) AS [fqn],
                                ty.name AS [dtype],
                                c.max_length AS [dataLength],
                                c.is_nullable AS [is_nullable],
                                CAST(epc.value AS NVARCHAR(MAX)) AS [description]
                            FROM sys.columns c
                            INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
                            LEFT JOIN sys.extended_properties epc 
                                ON epc.major_id = c.object_id 
                                AND epc.minor_id = c.column_id 
                                AND epc.class = 1
                            WHERE c.object_id = t.object_id
                            FOR JSON PATH)
                        ) AS [columns]
                    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                ) AS table_structure
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            LEFT JOIN sys.extended_properties ep 
                ON ep.major_id = t.object_id 
                AND ep.minor_id = 0 
                AND ep.class = 1
            WHERE s.name NOT IN ('information_schema', 'sys')
            AND t.type IN ('U', 'V') -- U = Table, V = View
            AND t.is_ms_shipped = 0 -- исключает системные объекты
            UNION ALL
            -- Для материализованных представлений (если нужно)
            SELECT 
                v.object_id AS oid,
                s.name AS schema_name,
                v.name AS table_name,
                'materialized_view' AS table_type,
                CAST(ep.value AS NVARCHAR(MAX)) AS description,
                (
                    SELECT 
                        JSON_QUERY(
                            (SELECT 
                                c.name AS [name],
                                CONCAT(DB_NAME(), '.', s.name, '.', v.name, '.', c.name) AS [fqn],
                                ty.name AS [dtype],
                                c.max_length AS [dataLength],
                                c.is_nullable AS [is_nullable],
                                CAST(epc.value AS NVARCHAR(MAX)) AS [description]
                            FROM sys.columns c
                            INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
                            LEFT JOIN sys.extended_properties epc 
                                ON epc.major_id = c.object_id 
                                AND epc.minor_id = c.column_id 
                                AND epc.class = 1
                            WHERE c.object_id = v.object_id
                            FOR JSON PATH)
                        ) AS [columns]
                    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                ) AS table_structure
            FROM sys.views v
            INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
            LEFT JOIN sys.extended_properties ep 
                ON ep.major_id = v.object_id 
                AND ep.minor_id = 0 
                AND ep.class = 1
            WHERE s.name NOT IN ('information_schema', 'sys')
            AND v.is_ms_shipped = 0
            ORDER BY schema_name, table_name;
        """;

        String url = buildDbUrl(source.getUrl(), dbName);
        LocalDateTime currentTime = LocalDateTime.now();

        try (Connection conn = DriverManager.getConnection(url, source.getUsername(), source.getPassword());
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            List<TableMetadata> entities = new ArrayList<>();

            while (rs.next()) {
                try {
                    TableMetadata entity = new TableMetadata();
                    String fqn = getFqn(List.of(source.getServiceName(), dbName, rs.getString("schema_name"), rs.getString("table_name")));
                    String parentFqn = fqn.substring(0, fqn.lastIndexOf("."));

                    EntityId id = new EntityId(rs.getLong("oid"),source.getServiceName());

                    entity.setId(id);
                    entity.setFqn(fqn);
                    entity.setDbName(dbName);
                    entity.setSchemaName(rs.getString("schema_name"));
                    entity.setDescription(rs.getString("description"));
                    entity.setName(rs.getString("table_name"));
                    entity.setParentFqn(parentFqn);
                    entity.setCreatedAt(currentTime);

                    // Собираем data (jsonb)
                    Map<String, Object> dataMap = new HashMap<>();

                    String jsonString = rs.getString("table_structure");
                    JsonNode columnsNode = objectMapper.readTree(jsonString);
                    dataMap.put("columns", columnsNode);

                    // Подсчет хэш
                    String jsonStringForHash = objectMapper.writeValueAsString(dataMap);
                    String hashString = fqn + rs.getString("description");
                    String hashData = DigestUtils.md5Hex(jsonStringForHash + hashString);
                    entity.setHashData(hashData);

                    JsonNode jsonNode = objectMapper.valueToTree(dataMap);
                    entity.setData(jsonNode);

                    entities.add(entity);
                } catch (JsonProcessingException e) {
                    log.error("Ошибка при преобразовании JSON для таблицы {}: {}", 
                            rs.getString("table_name"), e.getMessage(), e);
                }
            }
            tableRep.saveAll(entities);

        } catch (SQLException e) {
            log.error("Ошибка при получении таблиц для {}: {}", source.getName(), e.getMessage(), e);
        }
    }

    private String getFqn(List<String> names) {
        return String.join(".", names);
    }

    private String buildDbUrl(String originalUrl, String dbName) {
        String url = originalUrl.trim();
        
        if (url.contains("databaseName=")) {
            url = url.replaceAll("databaseName=[^;]+", "databaseName=" + dbName);
        } else {
            if (!url.endsWith(";")) {
                url += ";";
            }
            url += "databaseName=" + dbName;
        }
        
        return url;
    }
}
