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
//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PostgreSQLModeTest extends TestDataLoaderTest {

    @Container
    private PostgreSQLContainer postgresqlContainer = new PostgreSQLContainer<>("postgres:15.2-alpine")
            .withDatabaseName("test_data_loader")
            .withUsername("postgres")
            .withPassword("root")
            .withInitScript("createTables.sql");

    @BeforeEach
    public void setupDb()
    {
        try {
            this.connection = DriverManager.getConnection(postgresqlContainer.getJdbcUrl(), "postgres", "root");
            this.connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
//
//    @AfterEach
//    public void teardown() {
//        try {
//            this.connection.close();
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }
//    @Test
//    void dummyTest() {}

//    @Test
////    @ParameterizedTest
////    @MethodSource("paramsProvider")
////    void shouldLoadTheData_WhenDataFileIsFound(String dataPath, OperatingMode operatingMode) throws SQLException {
//    void shouldLoadTheData_WhenDataFileIsFound() throws SQLException, URISyntaxException, IOException {
////        this.connection = DriverManager.getConnection(postgresqlContainer.getJdbcUrl(), "postgres", "root");
//        TestDataLoader dataLoader = new TestDataLoader(connection, "src/test/resources/data/h2csvread", OperatingMode.POSTGRESQL_COPY);
//
//        assertAll(() -> assertEquals(0, runQueryForCount("select count(0) as count from client;")),
//                () -> assertEquals(0, runQueryForCount("select count(0) as count from second_table;")),
//                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;"))
//        );
//
////        long rowsInserted = new CopyManager((BaseConnection) this.connection)
////                .copyIn(
////                        "COPY client from STDIN (FORMAT csv, HEADER);",
////                        new BufferedReader(new FileReader(
////                                PostgreSQLModeTest.class.getClassLoader().getResource("data/h2csvread/1.client.csv").getPath()))
////                );
//        dataLoader.loadTables();
//
//        assertAll(() -> assertEquals(1, runQueryForCount("select count(0) as count from client;")),
//                () -> assertEquals(1, runQueryForCount("select count(0) as count from second_table;")),
//                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;"))
//        );
//    }

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
                Arguments.of("src/test/resources/data/h2csvread", OperatingMode.POSTGRESQL_COPY),
                Arguments.of("src/test/resources/data/customread", OperatingMode.CUSTOM)
        );
    }

    protected static Stream<Arguments> paramsNoSequenceProvider() {
        return Stream.of(
                Arguments.of("src/test/resources/data-no-sequence/h2csvread", OperatingMode.POSTGRESQL_COPY),
                Arguments.of("src/test/resources/data-no-sequence/customread", OperatingMode.CUSTOM)
        );
    }
}
