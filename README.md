# Сервис репликации метаданных из СУБД MsSQL

Сервис предназначен для репликации метаданных из базы данных Microsoft SQL Server.

## Сборка приложения

Для сборки приложения в JAR-файл выполните команду:

```bash
mvn package spring-boot:repackage -Dapp.version=${app.version}