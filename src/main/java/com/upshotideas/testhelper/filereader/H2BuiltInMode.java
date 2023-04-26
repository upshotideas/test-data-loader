package com.upshotideas.testhelper.filereader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

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
