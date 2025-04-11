## FileIO Implementation for Iceberg

Suggested usage in PySpark:
```text
(SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "jdbc")
    .config("spark.sql.catalog.local.jdbc.schema-version", "V1")
    .config("spark.sql.catalog.local.io-impl", "myorg.ice.LocalFileIO")
    .config("spark.sql.catalog.local.uri", "jdbc:hsqldb:file:hsql_db/ice_catalog")
    .config("spark.sql.catalog.local.warehouse", "ice-warehouse")
)
```
