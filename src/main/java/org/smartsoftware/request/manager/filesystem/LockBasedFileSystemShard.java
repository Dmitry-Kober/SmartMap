package org.smartsoftware.request.manager.filesystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartsoftware.domain.IValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by dkober on 24.4.2017 г..
 */
public class LockBasedFileSystemShard implements IFileSystemShard {

    private static final Logger LOG = LoggerFactory.getLogger(LockBasedFileSystemShard.class);

    private String shardLocation;

    LockBasedFileSystemShard(String shardLocation) {
        this.shardLocation = shardLocation;
    }

    @Override
    public void init() {
        LOG.trace("Initializing a File System for the: {} shard", shardLocation);

        try {
            Path shardPath = Paths.get(shardLocation);
            if (!Files.exists(shardPath)) {
                Files.createDirectories(shardPath);
            } else {
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
    public boolean createNewFileWithValue(IValue value) {
        return false;
    }

    @Override
    public IValue getValueFrom(Path filePath) {
        return null;
    }
}
