package com.upshotideas.testhelper;

import org.junit.jupiter.params.provider.Arguments;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

public class TestHelper {
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

    protected static Stream<Arguments> paramsProvider() {
        return Stream.of(
                Arguments.of("src/test/resources/data/h2csvread", OperatingMode.H2_BUILT_IN),
                Arguments.of("src/test/resources/data/customread", OperatingMode.CUSTOM)
        );
    }

    protected static Stream<Arguments> paramsNoSequenceProvider() {
        return Stream.of(
                Arguments.of("src/test/resources/data-no-sequence/h2csvread", OperatingMode.H2_BUILT_IN),
                Arguments.of("src/test/resources/data-no-sequence/customread", OperatingMode.CUSTOM)
        );
    }
}
