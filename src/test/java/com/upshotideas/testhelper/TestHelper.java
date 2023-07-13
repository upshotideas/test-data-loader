package com.upshotideas.testhelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

abstract public class TestHelper {
    protected Connection connection;

    protected int runQueryForCount(String sqlStmt) throws SQLException {
        try (ResultSet resultSet = runQuery(sqlStmt);) {
            return resultSet.getInt("count");
        }
    }

    protected ResultSet runQuery(String sqlStmt) throws SQLException {
        Statement statement = this.connection.createStatement();
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
