package com.leonardoramos.rootl_cdcpublisher.adapters.inbound.postgres;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class ReplicationConnectionFactory {

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
