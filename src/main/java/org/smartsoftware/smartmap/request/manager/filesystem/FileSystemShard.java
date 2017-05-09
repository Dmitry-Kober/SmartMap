package org.smartsoftware.smartmap.request.manager.filesystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final String TMP_FILE_SUFFIX = "_tmp";

    private final String shardLocation;

    public FileSystemShard(String shardLocation) {
        this.shardLocation = shardLocation;

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
    public void restore() {
        Set<String> tmpFiles = listAllFilesInShardMatching(Paths.get(shardLocation), ".*" + TMP_FILE_SUFFIX);
        tmpFiles.stream().forEach(path -> {
            String absTmpFilePath = shardLocation + "/" + path;
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
    public byte[] getValueFrom(Path path) {
        if ( !Files.exists(path) ) {
            return new byte[0];
        }

        byte[] data;
        try {
            data = Files.readAllBytes(path);
        }
        catch (IOException e) {
            LOG.error("Unable to read the '{}' file.", path.toFile().getAbsolutePath(), e);
            return new byte[0];
        }

        return data;
    }
}
