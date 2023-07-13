package com.upshotideas.testhelper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class PostgreSQLModeTest extends TestDataLoaderCommonTests {

    @Container
    private PostgreSQLContainer postgresqlContainer = new PostgreSQLContainer<>("postgres:15.2-alpine")
            .withDatabaseName("test_data_loader")
            .withUsername("postgres")
            .withPassword("root")
            .withInitScript("createTables.sql");

    @BeforeEach
    public void setupDb() {
        try {
            this.connection = DriverManager.getConnection(postgresqlContainer.getJdbcUrl(), "postgres", "root");
            this.connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest
    @MethodSource("paramsProvider")
    void shouldInsertJSONProperly(String dataPath, OperatingMode operatingMode) throws SQLException {
        TestDataLoader dataLoader = new TestDataLoader(connection, dataPath, operatingMode);
        dataLoader.loadTables();

        String actual = runQueryForSelectedStr("select json_col as selected_str from fourth_table;");
        assertAll(() -> assertEquals("{ \"region\":  \"us-east-2\" }",
                actual));
    }


    protected static Stream<Arguments> paramsProvider() {
        return Stream.of(
                Arguments.of("src/test/resources/data/csvread", OperatingMode.POSTGRESQL_COPY),
                Arguments.of("src/test/resources/data/customread", OperatingMode.CUSTOM)
        );
    }

    protected static Stream<Arguments> paramsNoSequenceProvider() {
        return Stream.of(
                Arguments.of("src/test/resources/data-no-sequence/csvread", OperatingMode.POSTGRESQL_COPY),
                Arguments.of("src/test/resources/data-no-sequence/customread", OperatingMode.CUSTOM)
        );
    }
}
