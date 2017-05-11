package org.smartsoftware.smartmap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartsoftware.smartmap.filesystem.FileSystemManager;
import org.smartsoftware.smartmap.filesystem.IFileSystemManager;
import org.smartsoftware.smartmap.utils.KeyedReentrantLock;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by dkober on 25.4.2017 г..
 */
public class SmartMap implements ISmartMap {

    public static final String WORKING_FOLDER = "repository";

    private static final Logger LOG = LoggerFactory.getLogger(SmartMap.class);
    private final KeyedReentrantLock<String> locks = new KeyedReentrantLock<>();

    private final IFileSystemManager fileSystemManager;

    public SmartMap() {
        this.fileSystemManager = new FileSystemManager(WORKING_FOLDER);
    }

    public SmartMap(IFileSystemManager fileSystemManager) {
        this.fileSystemManager = fileSystemManager;
    }

    @Override
    public byte[] get(String key) {
        final String filePath = buildDataFilePath(key);

        try {
            locks.readLock(filePath.intern());

            byte[] value = fileSystemManager.getValueFrom(Paths.get(filePath));
            if (value.length != 0) {
                return value;
            }
            else {
                return new byte[0];
            }
        }
        finally {
            locks.readUnlock(filePath);
        }
    }

    @Override
    public boolean put(String key, byte[] value) {
        final String filePath = buildDataFilePath(key);

        try {
            locks.writeLock(filePath.intern());

            boolean newFileAdded = fileSystemManager.createOrReplaceFileWithValue(Paths.get(filePath), value);
            if ( !newFileAdded ) {
                LOG.error("Unable to create a new file for the '{}' key.", key);
                return false;
            }

            return true;
        }
        finally {
            locks.writeUnlock(filePath);
        }
    }

    @Override
    public boolean remove(String key) {
        final String filePath = buildDataFilePath(key);

        try {
            locks.writeLock(filePath.intern());

            boolean filesIsRemoved = fileSystemManager.removeFile(Paths.get(filePath));
            if ( !filesIsRemoved ) {
                LOG.error("Unable to remove a file for the '{}' key.", key);
                return false;
            }

            return true;
        }
        finally {
            locks.writeUnlock(filePath);
        }
    }

    private String buildDataFilePath(String key) {
        return WORKING_FOLDER + "/" + key + ".data";
    }

    @Override
    public byte[] list() {
        File register = fileSystemManager.createRegister(Paths.get(WORKING_FOLDER));
        try {
            return Files.readAllBytes(register.toPath());
        }
        catch (IOException e) {
            return new byte[0];
        }
        finally {
            try {
                Files.deleteIfExists(register.toPath());
            }
            catch (IOException e) {
                LOG.error("Cannot delete the '{}' register file.", register.getAbsolutePath(), e);
            }
        }
    }
}
