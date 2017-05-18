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
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

/**
 * Created by dkober on 24.4.2017 г..
 */
public class FileSystemManager implements IFileSystemManager {

    private static final String DEFAULT_WORKING_FOLDER = "repository";
    private static final Logger LOG = LoggerFactory.getLogger(FileSystemManager.class);
    private static final String TMP_FILE_SUFFIX = "_tmp";
    private static final int FOLDER_NUMBER = 10;
    private static final int MAX_ATTEMPTS = 10;

    private final String workingFolder;

    public FileSystemManager() {
        this(DEFAULT_WORKING_FOLDER);
    }

    public FileSystemManager(String workingFolder) {
        LOG.trace("Initializing a File System for the: '{}' workingFolder", workingFolder);

        this.workingFolder = workingFolder;

        try {
            Path workingFolderPath = Paths.get(workingFolder);
            if (!Files.exists(workingFolderPath)) {
                Path directories = Files.createDirectories(workingFolderPath);
                directories.toFile().setReadable(true, true);
                directories.toFile().setWritable(true, true);
                directories.toFile().setExecutable(true, true);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getWorkingFolder() {
        return workingFolder;
    }

    @Override
    public void restore() {
        try {
            Files
                .find(Paths.get(getWorkingFolder()), 1, (path, basicFileAttributes) -> path.toFile().isDirectory() && path.toFile().getName().matches("folder_\\d+"))
                .forEach(path -> {
                    try {
                        Files
                            .find(path, Integer.MAX_VALUE, (folder, basicFileAttributes) -> folder.toFile().getName().matches(".*" + TMP_FILE_SUFFIX))
                            .forEach(folder -> {
                                try {
                                    Files.delete(folder);
                                }
                                catch (IOException e) {
                                    LOG.error("Cannot remove the '{}' temporary file.", folder.toFile().getAbsolutePath(), e);
                                }
                            });
                    } catch (IOException e) {
                        LOG.error("Cannot perform housekeeping in the '{}' folder.", path.toFile().getAbsolutePath(), e);
                    }
                });

        }
        catch (IOException e) {
            LOG.error("Cannot find working folders.", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean createOrReplaceFileFor(String key, byte[] value) {
        String dataAbsPath = buildDataFilePath(key);
        Path tmpFileAbsPath = Paths.get(buildTmpFilePath(dataAbsPath));

        try (OutputStream outputStream = Files.newOutputStream(tmpFileAbsPath)) {
            outputStream.write(value);
        }
        catch (IOException e) {
            LOG.error("Unable to create the '{}' temporary file. ", dataAbsPath, e);
            return false;
        }

        int counter = 0;
        while (counter++ < MAX_ATTEMPTS) {
            try {
                Files.move(tmpFileAbsPath, Paths.get(dataAbsPath), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                return true;
            }
            catch (IOException e) {
                LOG.error("Unable to atomically rename the '{}' temporary file. Attempt {} of {}.", dataAbsPath, counter, MAX_ATTEMPTS, e);
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e1) {
                    LOG.error("Interrupted while waiting for renaming the '{}' temporary file. Attempt {} of {}", dataAbsPath, counter, MAX_ATTEMPTS, e);
                    throw new RuntimeException(e1);
                }
            }
        }
        return false;
    }

    @Override
    public File createRegister() {
        try {
            Path workingFolderPath = Paths.get(getWorkingFolder());
            Files
                .find(workingFolderPath, 1, (path, basicFileAttributes) -> path.toFile().isDirectory() && path.toFile().getName().matches("folder_\\d+"))
                .flatMap(folder -> {
                    try {
                        return Files.find(folder, Integer.MAX_VALUE, (path, basicFileAttributes) -> path.toFile().getName().matches(".*data"));
                    }
                    catch (IOException e) {
                        LOG.error("Cannot build a register for the '' folder.", folder.toFile().getAbsolutePath(), e);
                        return Stream.empty();
                    }
                })
                .collect(new PathFileContentCollector(workingFolderPath));

            return Files
                    .find(workingFolderPath, Integer.MAX_VALUE, (path, basicFileAttributes) -> path.toFile().getName().matches(".*data"))
                    .collect(new PathFileContentCollector(workingFolderPath));
        }
        catch (IOException e) {
            LOG.error("Cannot create a local register for the '{}' workingFolder.", getWorkingFolder(), e);
            return new File(getWorkingFolder() + "/register_empty" + System.currentTimeMillis());
        }
    }

    @Override
    public boolean removeFileFor(String key) {
        Path path = Paths.get(buildDataFilePath(key));
        if (Files.exists(path)) {
            int counter = 0;
            while (counter++ < MAX_ATTEMPTS) {
                try {
                    Files.deleteIfExists(path);
                    return true;
                }
                catch (IOException e) {
                    LOG.error("Unable to remove the '{}' file. Attempt {} of {}.", path.toFile().getAbsolutePath(), counter, MAX_ATTEMPTS, e);
                }
            }
        }
        return false;
    }

    @Override
    public byte[] getValueFor(String key) {
        Path dataFilePath = Paths.get(buildDataFilePath(key));
        if ( !Files.exists(dataFilePath) ) {
            return new byte[0];
        }

        try {
            return Files.readAllBytes(dataFilePath);
        }
        catch (IOException e) {
            LOG.error("Unable to read the '{}' file.", dataFilePath.toFile().getAbsolutePath(), e);
            return new byte[0];
        }
    }

    private static String buildTmpFilePath(String absolutePath) {
        return absolutePath + "$" + System.currentTimeMillis() + TMP_FILE_SUFFIX;
    }

    private Path evaluateFolderFor(String key) {
        Path path =  Paths.get(getWorkingFolder() + "/folder_" + Math.abs(key.hashCode()) % FOLDER_NUMBER);
        if ( !Files.exists(path) ) {
            try {
                Path directories = Files.createDirectories(path);
                directories.toFile().setReadable(true, true);
                directories.toFile().setWritable(true, true);
                directories.toFile().setExecutable(true, true);
            }
            catch (IOException e) {
                LOG.error("Cannot create a folder for the '{}' key.", key);
                throw new RuntimeException(e);
            }
        }
        return path;
    }

    private String buildDataFilePath(String key) {
        return evaluateFolderFor(key).toFile().getAbsolutePath() + "/" + key + ".data";
    }
}
