package com.leonardoramos.rootl_cdcpublisher.adapters.inbound.postgres;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Fábrica de conexões para replicação do PostgreSQL. Esta classe é responsável por criar conexões configuradas para replicação, utilizando as propriedades específicas necessárias para estabelecer uma conexão de replicação com o banco de dados PostgreSQL.
 */
public class ReplicationConnectionFactory {

    /**
     * Cria uma conexão de replicação para o PostgreSQL utilizando as credenciais fornecidas.
     *
     * @param url      A URL de conexão do banco de dados PostgreSQL.
     * @param user     O nome de usuário para autenticação.
     * @param password A senha para autenticação.
     * @return Uma instância de PGConnection configurada para replicação.
     * @throws SQLException Se ocorrer um erro ao estabelecer a conexão.
     */
    public static PGConnection create(String url, String user, String password) throws SQLException {
        Properties props = new Properties();

        PGProperty.USER.set(props, user);
        PGProperty.PASSWORD.set(props, password);
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");

        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "16.0");

        org.postgresql.Driver driver = new org.postgresql.Driver();
        Connection connection = driver.connect(url, props);

        if (connection == null) {
            throw new SQLException("O driver nativo do Postgres falhou ao conectar na URL: " + url);
        }

        return connection.unwrap(PGConnection.class);
    }
}
