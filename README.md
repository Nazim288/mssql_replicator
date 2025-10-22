# Сервис репликации метаданных из СУБД MsSQL

Сервис предназначен для репликации метаданных из базы данных Microsoft SQL Server.

## Сборка приложения

Для сборки приложения в JAR-файл выполните команду:

```bash
mvn clean package -Dapp.version=${app.version}
```

## Запуск приложения

Для запуска приложения выполните команду:

```bash
java -jar mssql-replication-${app.version}.jar --spring.config.location=file:config/application.yaml
```
