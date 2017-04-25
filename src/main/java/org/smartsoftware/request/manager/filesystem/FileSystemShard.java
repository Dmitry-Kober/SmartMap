package org.smartsoftware.request.manager.filesystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartsoftware.domain.data.ByteArrayValue;
import org.smartsoftware.domain.data.IValue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by dkober on 24.4.2017 г..
 */
public class FileSystemShard implements IFileSystemShard {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemShard.class);

    private String shardLocation;

    FileSystemShard(String shardLocation) {
        this.shardLocation = shardLocation;
    }

    @Override
    public void init() {
        LOG.trace("Initializing a File System for the: {} shard", shardLocation);

        try {
            Path shardPath = Paths.get(shardLocation);
            if (!Files.exists(shardPath)) {
                Files.createDirectories(shardPath);
            }
            else {
                Files
                        .find(shardPath, Integer.MAX_VALUE, (path, basicFileAttributes) -> path.toFile().getName().matches(".*lock"))
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                throw new RuntimeException("Cannot remove file lock from the " + shardLocation + " shard.");
                            }
                        });
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean lockFile(Path path) {
        String absolutePath = path.toFile().getAbsolutePath();
        try {
            Files.createFile( Paths.get(absolutePath.substring(absolutePath.lastIndexOf(".")) + ".lock") );
        }
        catch (IOException e) {
            LOG.error("Unable to create the '{}' file. ", absolutePath, e);
            return false;
        }

        return true;
    }

    @Override
    public boolean unlockFile(Path path) {
        String absolutePath = path.toFile().getAbsolutePath();
        try {
            Files.delete( Paths.get(absolutePath.substring(absolutePath.lastIndexOf(".")) + ".lock") );
        }
        catch (IOException e) {
            LOG.error("Unable to create the '{}' file. ", absolutePath, e);
            return false;
        }

        return true;
    }

    @Override
    public boolean createNewFileWithValue(Path path, IValue value) {
        String absolutePath = path.toFile().getAbsolutePath();
        try {
            Files.createFile(path);
            value.get().ifPresent(data -> {
                try (OutputStream outputStream = Files.newOutputStream(path)) {
                    outputStream.write(value.get().orElse(new byte[0]));
                }
                catch (IOException e) {
                    LOG.error("Unable to enrich the '{}' file. ", absolutePath, e);
                }
            });
        }
        catch (IOException e) {
            LOG.error("Unable to create the '{}' file. ", absolutePath, e);
            return false;
        }

        return true;
    }

    @Override
    public boolean removeAllFilesWithMask(Path shardPath, String fileNameMask) {
        boolean[] filesAreRemoved = new boolean[1];
        filesAreRemoved[0] = false;

        try {
            Files
                    .find(shardPath, Integer.MAX_VALUE, (path, basicFileAttributes) -> path.toFile().getName().matches(fileNameMask))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                            filesAreRemoved[0] = true;
                        }
                        catch (IOException e) {
                            LOG.error("Unable to remove files from the '{}' shard with the '{}' mask.", new Object[] {shardLocation, fileNameMask}, e);
                        }
                    });
        }
        catch (IOException e) {
            LOG.error("Unable to remove files from the '{}' shard with the '{}' mask.", new Object[] {shardLocation, fileNameMask}, e);
            return false;
        }

        return filesAreRemoved[0];
    }

    @Override
    public IValue getValueFrom(Path path) {
        byte[] data;
        try {
            data = Files.readAllBytes(path);
        }
        catch (IOException e) {
            LOG.error("Unable to read the '{}' file.", path.toFile().getAbsolutePath(), e);
            return new ByteArrayValue();
        }

        return new ByteArrayValue(data);
    }
}
