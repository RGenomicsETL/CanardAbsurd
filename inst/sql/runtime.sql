SELECT version() AS duckdb_version,
    (SELECT extension_version FROM duckdb_extensions()
        WHERE extension_name = 'quack' AND loaded) AS quack_version,
    (SELECT version FROM canard_absurd.schema_version) AS schema_version;
