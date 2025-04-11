## FileIO Implementation for Iceberg

Suggested usage in PySpark:
```text
(SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "jdbc")
    .config("spark.sql.catalog.local.jdbc.schema-version", "V1")
    .config("spark.sql.catalog.local.io-impl", "myorg.ice.LocalFileIO")
    .config("spark.sql.catalog.local.uri", "jdbc:sqlite:sqlite-icedb")
    .config("spark.sql.catalog.local.warehouse", "ice-warehouse")
    .config("spark.sql.catalog.local.jdbc.init-catalog-tables", "false")  # true when jdbc db empty
)
```

Use sqlite JDBC as database
```text
<dependency>
  <groupId>org.xerial</groupId>
  <artifactId>sqlite-jdbc</artifactId>
  <version>3.46.1.3</version>
</dependency>
```
