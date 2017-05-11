package org.smartsoftware.smartmap.filesystem;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by dkober on 9.5.2017 г..
 */
public class FileSystemManagerTest {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemManagerTest.class);
    private static final String WORKING_FOLDER = "workingFolder1";

    @Test
    public void shouldInitializeFileSystemFromTmpFiles() throws IOException {
        IFileSystemManager fileSystemManager = new FileSystemManager(WORKING_FOLDER);

        String existingDataFilePath = WORKING_FOLDER + "/existing_file.data";
        createFileWithValue(existingDataFilePath, "existing", "Cannot create an exiting data file.");

        // create tmp files with outdated ones
        createFileWithValue(WORKING_FOLDER + "/existing_file.data$" + (System.currentTimeMillis() - 10) + "_tmp", "existing_overwritten_from_tmp_OUTDATED", "Cannot create an exiting outdated tmp file.");
        createFileWithValue(WORKING_FOLDER + "/existing_file.data$" + System.currentTimeMillis() + "_tmp", "existing_overwritten_from_tmp", "Cannot create an exiting tmp file.");
        createFileWithValue(WORKING_FOLDER + "/non_existing_file.data$" + (System.currentTimeMillis() - 10) + "_tmp", "non_existing_from_tmp_OUTDATED", "Cannot create a non-exiting outdated tmp file.");
        createFileWithValue(WORKING_FOLDER + "/non_existing_file.data$" + System.currentTimeMillis() + "_tmp", "non_existing_from_tmp", "Cannot create a non-exiting tmp file.");

        fileSystemManager.restore();

        assertThat(new String(getFileBytes(existingDataFilePath)), equalTo("existing_overwritten_from_tmp"));
        assertThat(new String(getFileBytes(WORKING_FOLDER + "/non_existing_file.data")), equalTo("non_existing_from_tmp"));
        assertTrue(Files.find(Paths.get(WORKING_FOLDER), Integer.MAX_VALUE, (path, basicFileAttributes) -> path.toFile().getName().matches(".*_tmp")).count() == 0);
    }

    private byte[] getFileBytes(String path) {
        return getFileBytes(path, "");
    }

    private byte[] getFileBytes(String path, String defaultValue) {
        try {
            return Files.readAllBytes(Paths.get(path));
        }
        catch (IOException e) {
            LOG.error("Cannot get content bytes from the '{}' file.", path, e);
            return defaultValue.getBytes();
        }
    }

    private void createFileWithValue(String path, String value, String errorMessagePrefix) {
        try (OutputStream outputStream = Files.newOutputStream(Paths.get(path))) {
            outputStream.write(value.getBytes());
        }
        catch (IOException e) {
            throw new RuntimeException(errorMessagePrefix, e);
        }
    }

    @After
    public void tearDown() throws IOException {
        Path workingFolderPath = Paths.get(WORKING_FOLDER);
        Files.list(workingFolderPath).forEach(file -> file.toFile().delete());
        Files.deleteIfExists(workingFolderPath);
    }
}
