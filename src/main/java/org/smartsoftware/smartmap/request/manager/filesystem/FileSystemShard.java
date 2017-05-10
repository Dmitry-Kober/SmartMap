package org.smartsoftware.smartmap.request.manager.filesystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartsoftware.smartmap.utils.PathFileContentCollector;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by dkober on 24.4.2017 г..
 */
public class FileSystemShard implements IFileSystemShard {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemShard.class);
    private static final String TMP_FILE_SUFFIX = "_tmp";

    private final String shardLocation;

    public FileSystemShard(String shardLocation) {
        this.shardLocation = shardLocation;

        LOG.trace("Initializing a File System for the: '{}' shard", shardLocation);

        try {
            Path shardPath = Paths.get(shardLocation);
            if (!Files.exists(shardPath)) {
                Files.createDirectories(shardPath);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void restore() {
        try {
            Files
                .find(Paths.get(shardLocation), Integer.MAX_VALUE, (path, basicFileAttributes) -> path.toFile().getName().matches(".*" + TMP_FILE_SUFFIX))
                .forEach(path -> {
                    String absTmpFilePath = path.toFile().getAbsolutePath();
                    Path tmpFilePath = Paths.get(absTmpFilePath);
                    byte[] tmpValue = new byte[0];
                    try {
                        tmpValue = Files.readAllBytes(tmpFilePath);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    Path valueFilePath = Paths.get(absTmpFilePath.substring(0, absTmpFilePath.lastIndexOf(TMP_FILE_SUFFIX)));
                    try (OutputStream outputStream = Files.newOutputStream(valueFilePath)) {
                        outputStream.write(tmpValue);
                    }
                    catch (IOException e) {
                        LOG.error("Unable to create the '{}' temporary file. ", valueFilePath, e);
                    }

                    try {
                        Files.deleteIfExists(tmpFilePath);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
        }
        catch (IOException e) {
            LOG.error("Cannot restore the '{}' shard from temporary files.", shardLocation, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean createOrReplaceFileWithValue(Path path, byte[] value) {
        String absolutePath = path.toFile().getAbsolutePath();
        try {
            Path tmpFileAbsPath = Paths.get(absolutePath + TMP_FILE_SUFFIX);

            try (OutputStream outputStream = Files.newOutputStream(tmpFileAbsPath)) {
                outputStream.write(value);
            }
            catch (IOException e) {
                LOG.error("Unable to create the '{}' temporary file. ", absolutePath, e);
                return false;
            }

            try (OutputStream outputStream = Files.newOutputStream(path)) {
                outputStream.write(value);
            }
            catch (IOException e) {
                LOG.error("Unable to enrich the '{}' file. ", absolutePath, e);
                return false;
            }

            Files.deleteIfExists(tmpFileAbsPath);
        }
        catch (IOException e) {
            LOG.error("Unable to create the '{}' file. ", absolutePath, e);
            return false;
        }

        return true;
    }

    @Override
    public File createShardRegister(Path shardPath) {
        try {
            return Files
                    .find(shardPath, Integer.MAX_VALUE, (path, basicFileAttributes) -> path.toFile().getName().matches(".*data"))
                    .collect(new PathFileContentCollector(shardPath));
        }
        catch (IOException e) {
            LOG.error("Cannot create a local register for the '{}' shard.", shardLocation, e);
            return new File(shardLocation + "/local_register_empty" + System.currentTimeMillis());
        }
    }

    @Override
    public boolean removeFile(Path path) {
        try {
            return Files.deleteIfExists(path);
        }
        catch (IOException e) {
            LOG.error("Unable to remove the '{}' file.", path.toFile().getAbsolutePath(), e);
            return false;
        }
    }

    @Override
    public byte[] getValueFrom(Path path) {
        if ( !Files.exists(path) ) {
            return new byte[0];
        }

        try {
            return Files.readAllBytes(path);
        }
        catch (IOException e) {
            LOG.error("Unable to read the '{}' file.", path.toFile().getAbsolutePath(), e);
            return new byte[0];
        }
    }
}
