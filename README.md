# Test Data Loader
Loads data from provided csv files into a given database, with simple controls for clearing or reloading data for all
your end-to-end test case scenarios.

# Features
1. loading data to a given DB.
2. Three operation modes: PostgreSQL, H2 and custom.
3. Native support for H2 database, which is common for test environments. (H2_BUILT_IN mode)
4. Native support for PostrgreSQL \Copy command (POSTGRESQL_COPY mode)
5. Ability to load calculated fields, like `now()` into the db (CUSTOM mode)
6. Simple APIs to load, clear or reload data.
7. Load/clear or reload all or selected tables.
8. Ability to define order of tables to load.
9. Can be used with any database that works with `java.sql.Connection`.

# How to use
## Add dependency
```xml
    <dependency>
        <groupId>com.upshotideas</groupId>
        <artifactId>test-data-loader</artifactId>
        <version>0.0.5</version>
    </dependency>
```
## Create CSV files
Put your CSV files in `src/test/resources/data`. Ensure that:
1. file names follow this pattern: `<order>.<tableName>.csv`
2. files follow standard CSV format: (refer to the tests in this project)
   1. comma separated
   2. double quotes to wrap strings
   3. use duplicate quotes to escape quotes (Ex: for a string containing quote `"McDonald''s"` or for json: `"{ ""region"":  ""us-east-2"" }"`)
   4. first row is a header row with columns matching those in the DB.

![data-location-screenshot.png](docs/data-location-screenshot.png)

## Construct and use the TestDataLoader
Assuming SpringBoot, the `src/test/resources/application.yml` would look like:
```yaml
spring:
  datasource:
    url: jdbc:h2:mem:test;MODE=PostgreSQL
    platform: h2
```
(Or you can use Postgres via test-containers' JDBC or Junit support!)

And a test class, ideally a parent test class, would look like:
```java
import javax.sql.DataSource;

@SpringBootTest
public class ExampleTest {
   protected TestDataLoader dataLoader = null;
   @Autowired private DataSource dataSource;

   @PostConstruct
   public void setUpClass() {
      dataLoader = new TestDataLoader(() -> DataSourceUtils.getConnection(dataSource));
   }

   @BeforeEach
   public void setUp() {
      dataLoader.loadTables();
   }

   @AfterEach
   public void teardown() {
      dataLoader.clearTables();
   }

   // Tests go here!
}
```
Or in case of Kotlin:
```kotlin
@SpringBootTest
class ExampleTest(private val dataSource: DataSource) {
    private var dataLoader: TestDataLoader? = null

    @PostConstruct
    fun setUpClass() {
        dataLoader = TestDataLoader { DataSourceUtils.getConnection(dataSource) }
    }

    @BeforeEach
    fun setUp() {
        dataLoader!!.loadTables()
    }

    @AfterEach
    fun teardown() {
        dataLoader!!.clearTables()
    }

   // Tests go here!
}
```

## Operating Modes
One can choose between two operating modes, which come with specific features and drawbacks!

### H2_BUILT_IN mode
The default mode, `H2_BUILT_IN` mode accepts standard, simple csv files. It relies on the H2 db's capability to
load data from a csv. A typical CSV file when using this mode can look like:
```csv
client_id, display_name, some_type, created_by, created_at, modified_by, modified_at
1, "some name", 'some type', "user", "2023-02-27", 'user', "2023-02-27"
```
This mode is directly using H2's `readcsv` function, and therefor is only available when using H2 database.
If you are using PostgreSQL, via docker/test-containers for testing, please check `POSTGRESQL_COPY` mode.
If you are using any other database, you would have to use the `CUSTOM` mode.

This mode will copy the CSV file as is, and does not support dynamic resolution of data like `CUSTOM` mode.

### POSTGRESQL_COPY mode
This mode accepts standard CSV files, similar to the `H2_BUILT_IN` mode. It relies on the PostgreSQL DB's capability
to read data from STDIN, which can contain CSV data. The typical file would look exactly as described above.

This mode is directly using Postgres client's `CopyManager` API, which is equivalent to the `\COPY` of the psql client. 
Therefore, this mode can only be used when Postgres DB is being used in application testing.

This mode will copy the CSV file as is, and does not support dynamic resolution of data like `CUSTOM` mode.

### Custom mode
Custom mode allows for using dynamic sql components to be used in the CSV, so they resolve when loading into
the database. This is useful when using time based or relation based values as input. You can do things like:
```csv
client_id, display_name, some_type, created_by, created_at, modified_by, modified_at
1, "some name", 'some type', "user", "__sql__NOW()__sql__", 'user', "__sql__NOW()__sql__"
```
The key here is wrapping the SQL in `__sql__` strings. The column is assumed to be of a
string/varchar type and is processed by removing the `__sql__` markers.
This mode also allows some DB-valid, but CSV-not-valid values, like NULL to a timestamp field! (where H2_BUILT_IN fails.)

This mode can be used with any database, H2 or otherwise. But, getting the SQL right may seem a bit tricky,
especially with quotes in them.

### Data with Quotes or JSON
You may need to escape quotes if the literal contains quotes, or if you are using json data. Here is an example:
```csv
literal_with_quotes, some_type, json_data
"I have quotes ''haha'', see?", 'some type', "{ ""region"":  ""us-east-2"" }"
```
