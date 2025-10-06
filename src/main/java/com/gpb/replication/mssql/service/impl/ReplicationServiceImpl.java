package com.gpb.replication.mssql.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.replication.mssql.dto.SourceDbConnections;
import com.gpb.replication.mssql.log.SvoiCustomLogger;
import com.gpb.replication.mssql.log.SvoiSeverityEnum;
import com.gpb.replication.mssql.model.DatabaseMetadata;
import com.gpb.replication.mssql.model.EntityId;
import com.gpb.replication.mssql.model.SchemaMetadata;
import com.gpb.replication.mssql.model.TableMetadata;
import com.gpb.replication.mssql.properties.SqlTemplates;
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

    private final DatabaseMetadataRepository databaseRep;
    private final SchemaMetadataRepository schemaRep;
    private final TableMetadataRepository tableRep;

    private final SqlTemplates sqlTemplates;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Async
    public void startReplicationAsync(String serviceName) {
        startReplication(serviceName);
    }

    @Override
    public void startReplication(String serviceName) {
        truncateTables(serviceName);

        Map<String, SourceDbConnections> dbConnectionsMap = dbSourcesService.getDbConnections()
                .stream()
                .collect(Collectors.toMap(SourceDbConnections::getName, Function.identity()));

        if (!dbConnectionsMap.containsKey(serviceName)) {
            log.warn("Не найдены данные по сервису {} для репликации", serviceName);
            return;
        }

        SourceDbConnections source = dbConnectionsMap.get(serviceName);

        List<String> databases = databaseReplication(source);

        for (String dbName : databases) {
            schemaReplication(source, dbName);
            tableReplication(source, dbName);
        }

        log.info("Репликация MSSQL завершена успешно для {}", serviceName);
        svoiCustomLogger.send(
                "replicationJob",
                "Replication Finished",
                String.format("Replicated MSSQL source: [%s];", serviceName),
                SvoiSeverityEnum.ONE
        );
    }

    private void truncateTables(String serviceName) {
        databaseRep.deleteByServiceName(serviceName);
        schemaRep.deleteByServiceName(serviceName);
        tableRep.deleteByServiceName(serviceName);
    }

    private List<String> databaseReplication(SourceDbConnections source) {
        List<String> response = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();

        try (Connection conn = DriverManager.getConnection(source.getUrl(), source.getUsername(), source.getPassword());
             PreparedStatement stmt = conn.prepareStatement(sqlTemplates.getDatabaseSql());
             ResultSet rs = stmt.executeQuery()) {

            List<DatabaseMetadata> entities = new ArrayList<>();
            while (rs.next()) {
                String dbName = rs.getString("datname");
                String fqn = String.join(".", source.getServiceName(), dbName);

                EntityId id = new EntityId(rs.getLong("oid"), source.getServiceName());

                DatabaseMetadata entity = new DatabaseMetadata();
                entity.setId(id);
                entity.setFqn(fqn);
                entity.setName(dbName);
                entity.setServiceName(source.getServiceName());
                entity.setCreatedAt(now);
                entity.setHashData(DigestUtils.md5Hex(fqn));

                entities.add(entity);
                response.add(dbName);
            }
            databaseRep.saveAll(entities);

        } catch (SQLException e) {
            log.error("Ошибка при получении баз для {}: {}", source.getName(), e.getMessage(), e);
        }
        return response;
    }

    private void schemaReplication(SourceDbConnections source, String dbName) {
        String url = buildDbUrl(source.getUrl(), dbName);
        LocalDateTime now = LocalDateTime.now();

        try (Connection conn = DriverManager.getConnection(url, source.getUsername(), source.getPassword());
             PreparedStatement stmt = conn.prepareStatement(sqlTemplates.getSchemaSql());
             ResultSet rs = stmt.executeQuery()) {

            List<SchemaMetadata> entities = new ArrayList<>();
            while (rs.next()) {
                String schemaName = rs.getString("schema_name");
                String fqn = String.join(".", source.getServiceName(), dbName, schemaName);

                // делаем parentFqn уникальным на уровне базы
                String parentFqn = source.getServiceName() + "." + dbName;

                EntityId id = new EntityId(rs.getLong("oid"), parentFqn);

                SchemaMetadata entity = new SchemaMetadata();
                entity.setId(id);
                entity.setFqn(fqn);
                entity.setDbName(dbName);
                entity.setName(schemaName);
                entity.setServiceName(source.getServiceName());
                entity.setCreatedAt(now);
                entity.setHashData(DigestUtils.md5Hex(fqn));

                entities.add(entity);
            }
            schemaRep.saveAll(entities);

        } catch (SQLException e) {
            log.error("Ошибка при получении схем для {}: {}", source.getName(), e.getMessage(), e);
        }
    }

    private void tableReplication(SourceDbConnections source, String dbName) {
        String url = buildDbUrl(source.getUrl(), dbName);
        LocalDateTime now = LocalDateTime.now();

        try (Connection conn = DriverManager.getConnection(url, source.getUsername(), source.getPassword());
             PreparedStatement stmt = conn.prepareStatement(sqlTemplates.getTableSql());
             ResultSet rs = stmt.executeQuery()) {

            List<TableMetadata> entities = new ArrayList<>();
            while (rs.next()) {
                try {
                    String schemaName = rs.getString("schema_name");
                    String tableName = rs.getString("table_name");

                    String fqn = String.join(".", source.getServiceName(), dbName, schemaName, tableName);

                    // parentFqn включает базу и схему
                    String parentFqn = source.getServiceName() + "." + dbName + "." + schemaName;

                    EntityId id = new EntityId(rs.getLong("oid"), parentFqn);

                    TableMetadata entity = new TableMetadata();
                    entity.setId(id);
                    entity.setFqn(fqn);
                    entity.setDbName(dbName);
                    entity.setSchemaName(schemaName);
                    entity.setName(tableName);
                    entity.setServiceName(source.getServiceName());
                    entity.setDescription(rs.getString("description"));
                    entity.setCreatedAt(now);

                    String jsonString = rs.getString("table_structure");
                    JsonNode columnsNode = objectMapper.readTree(jsonString);
                    String hashString = fqn + rs.getString("description");
                    String hashData = DigestUtils.md5Hex(jsonString + hashString);
                    entity.setHashData(hashData);

                    JsonNode jsonNode = objectMapper.valueToTree(columnsNode);
                    entity.setData(jsonNode);

                    entities.add(entity);

                } catch (Exception e) {
                    log.error("Ошибка при обработке таблицы {}: {}", rs.getString("table_name"), e.getMessage(), e);
                }
            }
            tableRep.saveAll(entities);

        } catch (SQLException e) {
            log.error("Ошибка при получении таблиц для {}: {}", source.getName(), e.getMessage(), e);
        }
    }


    private String buildDbUrl(String originalUrl, String dbName) {
        if (originalUrl.contains("databaseName=")) {
            return originalUrl.replaceAll("databaseName=[^;]+", "databaseName=" + dbName);
        }
        return originalUrl + ";databaseName=" + dbName;
    }
}
