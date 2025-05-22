package com.upshotideas.testhelper;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Supplier;

abstract public class TestHelper {
    private HikariDataSource dataSource;
    protected Supplier<Connection> connectionSupplier;

    protected void setupDb(String url, String userName, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(userName);
        config.setPassword(password);
        this.dataSource = new HikariDataSource(config);

        this.connectionSupplier = () -> {
            try {
                return dataSource.getConnection();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    protected void tearDownDb() {
        this.dataSource.close();
    }

    protected int runQueryForCount(String sqlStmt) throws SQLException {
        try (Connection connection = this.connectionSupplier.get();
             Statement statement = connection.createStatement();
             ResultSet resultSet = runQuery(sqlStmt, statement)) {
            return resultSet.getInt("count");
        }
    }

    protected ResultSet runQuery(String sqlStmt, Statement statement) throws SQLException {
        ResultSet resultSet = statement.executeQuery(sqlStmt);
        resultSet.next();
        return resultSet;
    }

    protected String runQueryForSelectedStr(String sqlStmt) throws SQLException {
        try (Connection connection = this.connectionSupplier.get();
             Statement statement = connection.createStatement();
             ResultSet resultSet = runQuery(sqlStmt, statement)) {
            return resultSet.getString("selected_str");
        }
    }
}
