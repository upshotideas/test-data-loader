package com.upshotideas.testhelper.filereader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * This class loads csv data into an H2 database. It does not yet check if the connection provided is to an instance of
 * H2, just assumes that it is, if something is using this mode.
 * <p />
 * It uses the csvread function from H2. It can only import the data as-is, if it exists in a valid csv format.
 * It assumes that the provided CSV has headers.
 */
public class H2BuiltInMode implements IOperatingMode {
    @Override
    public List<String> generateTableSql(Map.Entry<String, Path> e) throws IOException {
        try (Stream<String> fileLines = Files.lines(e.getValue());) {
            Optional<String> optionalCols = fileLines.findFirst();

            return optionalCols.map(s -> Arrays.asList(e.getKey(), "insert into " + e.getKey() + "(" + s +
                            ") select * from CSVREAD('" + e.getValue() + "',null,'charset=UTF-8');"))
                    .orElseGet(() -> Arrays.asList(e.getKey(), ""));

        }
    }
}
