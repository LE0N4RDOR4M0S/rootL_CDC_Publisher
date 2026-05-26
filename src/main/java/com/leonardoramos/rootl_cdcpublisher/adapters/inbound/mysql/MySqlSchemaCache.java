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

/**
 * Cache simples para armazenar o schema das tabelas do MySQL, evitando consultas repetidas ao banco de dados.
 * O cache é atualizado na primeira consulta e pode ser invalidado se necessário.
 */
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

    /**
     * Retorna a lista de colunas de uma tabela específica, utilizando o cache se disponível ou carregando do banco de dados se necessário.
     * @param database Nome do banco de dados
     * @param table Nome da tabela
     * @return Lista de nomes de colunas da tabela
     */
    public List<String> getColumns(String database, String table) {
        String key = database + "." + table;
        return tableColumnsCache.computeIfAbsent(key, k -> loadColumnsFromDb(database, table));
    }

    /**
     * Carrega a lista de colunas de uma tabela do banco de dados, ordenada pela posição ordinal, e atualiza o cache.
     * @param database Nome do banco de dados
     * @param table Nome da tabela
     * @return Lista de nomes de colunas da tabela
     */
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
