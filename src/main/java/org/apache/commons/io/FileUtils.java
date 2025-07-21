package org.apache.commons.io;

import java.io.File;
import java.io.IOException;

/**
 * Placeholder for the Apache Commons IO FileUtils class.
 */
public class FileUtils {
    /**
     * Simulates the behavior of creating a directory, including parent directories.
     * @param directory The directory to create.
     * @throws IOException if the directory cannot be created.
     */
    public static void forceMkdir(File directory) throws IOException {
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw new IOException("Cannot create directory: " + directory);
            }
        }
    }
}