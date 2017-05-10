package org.smartsoftware.smartmap.request.manager.filesystem;

import org.junit.After;
import org.junit.Before;
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
public class FileSystemShardTest {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemShardTest.class);
    private static final String SHARD_PATH = "shard1";

    @Before
    public void setUp() throws IOException {
        Path shardPath = Paths.get(SHARD_PATH);
        if (Files.exists(shardPath)) {
            Files.list(shardPath).forEach(file -> file.toFile().delete());
        }
    }

    @Test
    public void shouldInitializeFileSystemFromTmpFiles() throws IOException {
        IFileSystemShard fileSystemShard = new FileSystemShard(SHARD_PATH);

        String existingDataFilePath = SHARD_PATH + "/existing_file.data";
        createFileWithValue(existingDataFilePath, "existing", "Cannot create an exiting tmp file.");
        createFileWithValue(SHARD_PATH + "/existing_file.data_tmp", "existing_overwritten_from_tmp", "Cannot create an exiting file.");
        createFileWithValue(SHARD_PATH + "/non_existing_file.data_tmp", "non_existing_from_tmp", "Cannot create a non-exiting tmp file.");

        fileSystemShard.restore();

        assertThat(new String(getFileBytes(existingDataFilePath, "")), equalTo("existing_overwritten_from_tmp"));
        assertThat(new String(getFileBytes(SHARD_PATH + "/non_existing_file.data", "")), equalTo("non_existing_from_tmp"));
        assertTrue(Files.find(Paths.get(SHARD_PATH), Integer.MAX_VALUE, (path, basicFileAttributes) -> path.toFile().getName().matches(".*_tmp")).count() == 0);
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
        Path shardPath = Paths.get(SHARD_PATH);
        Files.list(shardPath).forEach(file -> file.toFile().delete());
        Files.deleteIfExists(shardPath);
    }
}
