package com.leonardoramos.rootl_cdcpublisher.infrastructure.config;

public class AppConfig {

    public static final String KAFKA_BOOTSTRAP_SERVERS = getEnv("KAFKA_BOOTSTRAP_SERVERS", "${kafka.bootstrap-servers}");

    public static final String PG_JDBC_URL = getEnv("PG_JDBC_URL", "${postgres.jdbc-url");
    public static final String PG_USER = getEnv("PG_USER", "${postgres.user}");
    public static final String PG_PASSWORD = getEnv("PG_PASSWORD", "${postgres.password}");
    public static final String PG_SLOT_NAME = getEnv("PG_SLOT_NAME", "${postgres.slot-name}");
    public static final String PG_DATABASE = getEnv("PG_DATABASE", "${postgres.database}");

    public static final String OFFSET_DIR = getEnv("OFFSET_DIR", "./offsets");
    public static final String CONNECTOR_ID = getEnv("CONNECTOR_ID", "postgres-financeiro");

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}
