package org.smartsoftware.smartmap.filesystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartsoftware.smartmap.utils.PathFileContentCollector;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

/**
 * Created by dkober on 24.4.2017 г..
 */
public class FileSystemManager implements IFileSystemManager {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemManager.class);
    private static final String TMP_FILE_SUFFIX = "_tmp";

    private final String workingFolderLocation;

    public FileSystemManager(String workingFolderLocation) {
        this.workingFolderLocation = workingFolderLocation;

        LOG.trace("Initializing a File System for the: '{}' workingFolder", workingFolderLocation);

        try {
            Path workingFolderPath = Paths.get(workingFolderLocation);
            if (!Files.exists(workingFolderPath)) {
                Files.createDirectories(workingFolderPath);
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
                .find(Paths.get(workingFolderLocation), Integer.MAX_VALUE, (path, basicFileAttributes) -> path.toFile().getName().matches(".*" + TMP_FILE_SUFFIX))
                .sorted((path1, path2) -> (-1) * path1.toFile().getAbsolutePath().compareTo(path2.toFile().getAbsolutePath())) // descending order
                .collect(
                    Collectors.groupingBy(
                            path -> {
                                String tmpAbsPath = path.toFile().getAbsolutePath();
                                return tmpAbsPath.substring(1, tmpAbsPath.lastIndexOf("$"));
                            }
                    )
                )
                .entrySet().stream()
                .map(entry -> {
                    Path targetTmpFile = entry.getValue().remove(0);
                    entry.getValue().stream().forEach(this::removeFile);
                    return targetTmpFile;
                })
                .forEach(path -> {
                    String absTmpFilePath = path.toFile().getAbsolutePath();
                    Path tmpFilePath = Paths.get(absTmpFilePath);

                    Path valueFilePath = Paths.get(absTmpFilePath.substring(0, absTmpFilePath.lastIndexOf("$")));
                    try (OutputStream outputStream = Files.newOutputStream(valueFilePath)) {
                        outputStream.write( Files.readAllBytes(tmpFilePath) );
                    }
                    catch (IOException e) {
                        LOG.error("Unable to move data from the '{}' temporary file. ", valueFilePath, e);
                    }

                    removeFile(tmpFilePath);
                });
        }
        catch (IOException e) {
            LOG.error("Cannot restore the '{}' workingFolder from temporary files.", workingFolderLocation, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean createOrReplaceFileWithValue(Path path, byte[] value) {
        String absolutePath = path.toFile().getAbsolutePath();

        Path tmpFileAbsPath = Paths.get(absolutePath + "$" + System.currentTimeMillis() + TMP_FILE_SUFFIX);

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

        removeFile(tmpFileAbsPath);

        return true;
    }

    @Override
    public File createRegister(Path workingFolderPath) {
        try {
            return Files
                    .find(workingFolderPath, Integer.MAX_VALUE, (path, basicFileAttributes) -> path.toFile().getName().matches(".*data"))
                    .collect(new PathFileContentCollector(workingFolderPath));
        }
        catch (IOException e) {
            LOG.error("Cannot create a local register for the '{}' workingFolder.", workingFolderLocation, e);
            return new File(workingFolderLocation + "/register_empty" + System.currentTimeMillis());
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
