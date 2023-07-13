package com.upshotideas.testhelper.filereader;

import com.upshotideas.testhelper.CopyOperation;
import com.upshotideas.testhelper.Functions;
import com.upshotideas.testhelper.TableOperationTuple;
import com.upshotideas.testhelper.TestDataLoaderException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * This class loads csv data into an H2 database. It does not yet check if the connection provided is to an instance of
 * H2, just assumes that it is, if something is using this mode.
 * <p>
 * It uses the csvread function from H2. It can only import the data as-is, if it exists in a valid csv format.
 * It assumes that the provided CSV has headers.
 */
class H2BuiltInMode implements IOperatingMode {
    @Override
    public TableOperationTuple generateTableSql(Map.Entry<String, Path> e) throws IOException {
        try (Stream<String> fileLines = Files.lines(e.getValue());) {
            Optional<String> optionalCols = fileLines.findFirst();

            return optionalCols.map(s -> new TableOperationTuple(e.getKey(), this.generateCopyOperation(s, e.getKey(), e.getValue())))
                    .orElseGet(() -> Functions.getNoopOperation(e.getKey()));

        }
    }

    private CopyOperation generateCopyOperation(String columns, String tableName, Path value) {
        return connection -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("insert into " + tableName + "(" + columns +
                        ") select * from CSVREAD('" + value + "',null,'charset=UTF-8');");
                connection.commit();
            } catch (SQLException e) {
                throw new TestDataLoaderException(e);
            }
        };
    }
}
