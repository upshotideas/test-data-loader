package com.upshotideas.testhelper.filereader;

import com.upshotideas.testhelper.CopyOperation;
import com.upshotideas.testhelper.Functions;
import com.upshotideas.testhelper.TableOperationTuple;
import com.upshotideas.testhelper.TestDataLoaderException;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.PgConnection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * This class loads CSV data into postgres DB, using postgres's \COPY command. Note that this is not equivalent to
 * the COPY command, but the \COPY command instead; equivalent to using \COPY on a psql client.
 * <p>
 * This mode functions almost similar to the H2 mode, except the difference in exhibited behaviour,
 * like in JSON handling.
 * <p>
 * This mode can be used only when postgres driver is on the classpath.
 */
public class PostgreSQLBuiltInMode implements IOperatingMode {
    @Override
    public TableOperationTuple generateTableSql(Map.Entry<String, Path> e) throws IOException {
        try (Stream<String> fileLines = Files.lines(e.getValue())) {
            Optional<String> optionalCols = fileLines.findFirst();

            return optionalCols.map(s -> new TableOperationTuple(e.getKey(), this.generateCopyOperation(s, e.getKey(), e.getValue())))
                    .orElseGet(() -> Functions.getNoopOperation(e.getKey()));

        }
    }

    private CopyOperation generateCopyOperation(String columns, String tableName, Path value) {
        return connectionSupplier -> {
            try (Connection connection = connectionSupplier.getConnection()) {
                new CopyManager(connection.unwrap(PgConnection.class))
                        .copyIn("COPY " + tableName + "(" + columns +
                                        ") from STDIN (FORMAT csv, HEADER);",
                                new BufferedReader(new FileReader(value.toFile())));
            } catch (SQLException | IOException e) {
                throw new TestDataLoaderException(e);
            }
        };
    }
}
