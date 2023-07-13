package com.upshotideas.testhelper.filereader;

import com.upshotideas.testhelper.CopyOperation;
import com.upshotideas.testhelper.Functions;
import com.upshotideas.testhelper.TableOperationTuple;
import com.upshotideas.testhelper.TestDataLoaderException;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

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
        return connection -> {
            try {
                new CopyManager((BaseConnection) connection)
                        .copyIn("COPY " + tableName + "(" + columns +
                                ") from STDIN (FORMAT csv, HEADER);",
                                new BufferedReader(new FileReader(value.toFile())));
            } catch (SQLException | IOException e) {
                throw new TestDataLoaderException(e);
            }
        };
    }
}
