# Database Dialects

## What are database dialects?
Database dialects help the AWS Advanced Go Wrapper determine what kind of underlying database is being used. To function correctly, the AWS Advanced Go Wrapper requires details unique to specific databases such as the default port number or the method to get the current host from the database. These details can be defined and provided to the AWS Advanced Go Wrapper by using database dialects.

## Configuration Parameters
| Name             | Required            | Description                                                                        | Example                                       |
|------------------|---------------------|------------------------------------------------------------------------------------|-----------------------------------------------|
| `databaseDialect` | No (see note below) | The [dialect code](#list-of-available-dialect-codes) of the desired database type. | `aurora-mysql` |

> [!NOTE]
> The `databaseDialect` parameter is not required. When it is not provided by the user, the AWS Advanced Go Wrapper will attempt to determine which of the existing dialects to use based on other connection details from the DSN. However, if the dialect is known by the user, it is preferable to set the `databaseDialect` parameter because it will take time to resolve the dialect.

### List of Available Dialect Codes
Dialect codes specify what kind of database any connections will be made to.

| Dialect Code                 | Database                                                                                                                                           |
|------------------------------| -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `aurora-mysql`               | Aurora MySQL                                                                                                                                       |
| `rds-multi-az-mysql-cluster` | [Amazon RDS MySQL Multi-AZ DB Cluster Deployments](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html)      |
| `rds-mysql`                  | Amazon RDS MySQL                                                                                                                                   |
| `mysql`                      | MySQL                                                                                                                                              |
| `aurora-pg`                  | Aurora PostgreSQL                                                                                                                                  |
| `rds-multi-az-pg-cluster`    | [Amazon RDS PostgreSQL Multi-AZ DB Cluster Deployments](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html) |
| `rds-pg`                     | Amazon RDS PostgreSQL                                                                                                                              |
| `pg`                         | PostgreSQL                                                                                                                                         |
