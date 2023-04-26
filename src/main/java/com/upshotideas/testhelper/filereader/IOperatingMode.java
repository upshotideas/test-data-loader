package com.upshotideas.testhelper.filereader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public interface IOperatingMode {
    public List<String> generateTableSql(Map.Entry<String, Path> e) throws IOException;
}
