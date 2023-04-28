package com.upshotideas.testhelper.filereader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Operating mode allows different ways of reading and converting the CSV files to insert statements.
 * This is a class for internal library use, you cannot add operating modes from outside.
 */
public interface IOperatingMode {
    /**
     * Implementing classes of this method are expected to return the CSV file data as sql (insert) statements.
     * The returned list value is expected to have two elements, first being the name of the file and second the sql.
     *
     * @param mapEntry entry containing name of the table and path to the file
     * @return List containing name of the table and the file data in SQL format
     * @throws IOException If any issues happen when trying to read the file
     */

    public List<String> generateTableSql(Map.Entry<String, Path> mapEntry) throws IOException;
}
