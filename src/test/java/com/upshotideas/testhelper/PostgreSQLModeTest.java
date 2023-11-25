package com.upshotideas.testhelper;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
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
        super.setupDb(postgresqlContainer.getJdbcUrl(), "postgres", "root");
    }

    @AfterEach
    public void tearDownDb() {
        super.tearDownDb();
    }


    @ParameterizedTest
    @MethodSource("paramsProvider")
    void shouldInsertJSONProperly(String dataPath, OperatingMode operatingMode) throws SQLException {
        TestDataLoader dataLoader = new TestDataLoader(connectionSupplier, dataPath, operatingMode);
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
