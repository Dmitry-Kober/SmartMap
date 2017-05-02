package org.smartsoftware.smartmap.request.manager.filesystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartsoftware.smartmap.domain.data.ByteArrayValue;
import org.smartsoftware.smartmap.domain.data.IValue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

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
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean createOrReplaceFileWithValue(Path path, IValue value) {
        String absolutePath = path.toFile().getAbsolutePath();
        try {
            Files.deleteIfExists(path);
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
    public Set<String> listAllFilesInShardMatching(Path shardPath, String fileNameMask) {
        try {
            return Files
                    .find(shardPath, Integer.MAX_VALUE, (path, basicFileAttributes) -> path.toFile().getName().matches(fileNameMask))
                    .map(path -> path.toFile().getName())
                    .collect(Collectors.toSet());
        }
        catch (IOException e) {
            LOG.error("Unable to remove files from the '{}' shard with the '{}' mask.", new Object[] {shardLocation, fileNameMask}, e);
            return Collections.emptySet();
        }
    }

    @Override
    public boolean removeFile(Path path) {
        try {
            return Files.deleteIfExists(path);
        }
        catch (IOException e) {
            LOG.error("Unable to remove the '{}' file from the '{}' shard .", new Object[]{path.toFile().getAbsolutePath(), shardLocation}, e);
            return false;
        }
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
