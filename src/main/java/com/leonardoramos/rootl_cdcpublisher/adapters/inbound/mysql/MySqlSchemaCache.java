package com.leonardoramos.rootl_cdcpublisher.adapters.inbound.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MySqlSchemaCache {
    private static final Logger log = LoggerFactory.getLogger(MySqlSchemaCache.class);

    private final String jdbcUrl;
    private final String user;
    private final String password;

    private final Map<String, List<String>> tableColumnsCache = new ConcurrentHashMap<>();

    public MySqlSchemaCache(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    public List<String> getColumns(String database, String table) {
        String key = database + "." + table;
        return tableColumnsCache.computeIfAbsent(key, k -> loadColumnsFromDb(database, table));
    }

    private List<String> loadColumnsFromDb(String database, String table) {
        List<String> columns = new ArrayList<>();
        String query = "SELECT COLUMN_NAME FROM information_schema.columns " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             var stmt = conn.prepareStatement(query)) {
            stmt.setString(1, database);
            stmt.setString(2, table);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    columns.add(rs.getString("COLUMN_NAME"));
                }
            }
            log.info("Cache de schema atualizado para a tabela: {}.{}", database, table);
        } catch (Exception e) {
            log.error("Falha ao carregar colunas para a tabela {}.{}", database, table, e);
        }
        return columns;
    }
}
