package com.upshotideas.testhelper;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

abstract public class TestHelper {
    private HikariDataSource dataSource;
    protected ConnectionSupplier connectionSupplier;

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
        try (ResultSet resultSet = runQuery(sqlStmt);) {
            return resultSet.getInt("count");
        }
    }

    protected ResultSet runQuery(String sqlStmt) throws SQLException {
        Statement statement = this.connectionSupplier.getConnection().createStatement();
        ResultSet resultSet = statement.executeQuery(sqlStmt);
        resultSet.next();
        return resultSet;
    }

    protected String runQueryForSelectedStr(String sqlStmt) throws SQLException {
        try (ResultSet resultSet = runQuery(sqlStmt);) {
            return resultSet.getString("selected_str");
        }
    }
}
