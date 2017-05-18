package org.smartsoftware.smartmap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartsoftware.smartmap.filesystem.FileSystemManager;
import org.smartsoftware.smartmap.filesystem.IFileSystemManager;
import org.smartsoftware.smartmap.utils.KeyedReentrantLock;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Created by dkober on 25.4.2017 г..
 */
public class SmartMap implements ISmartMap {

    private static final Logger LOG = LoggerFactory.getLogger(SmartMap.class);
    private final KeyedReentrantLock<String> locks = new KeyedReentrantLock<>();

    private final IFileSystemManager fileSystemManager;

    public SmartMap() {
        this.fileSystemManager = new FileSystemManager();
    }

    public SmartMap(IFileSystemManager fileSystemManager) {
        this.fileSystemManager = fileSystemManager;
    }

    @Override
    public byte[] get(String key) {
        byte[] value = fileSystemManager.getValueFor(key);
        if (value.length != 0) {
            return value;
        }
        else {
            return new byte[0];
        }
    }

    @Override
    public boolean put(String key, byte[] value) {
        try {
            locks.writeLock(key.intern());

            boolean newFileAdded = fileSystemManager.createOrReplaceFileFor(key, value);
            if ( !newFileAdded ) {
                LOG.error("Unable to create a new file for the '{}' key.", key);
                return false;
            }

            return true;
        }
        finally {
            locks.writeUnlock(key);
        }
    }

    @Override
    public boolean remove(String key) {
        try {
            locks.writeLock(key.intern());

            boolean filesIsRemoved = fileSystemManager.removeFileFor(key);
            if ( !filesIsRemoved ) {
                LOG.error("Unable to remove a file for the '{}' key.", key);
                return false;
            }

            return true;
        }
        finally {
            locks.writeUnlock(key);
        }
    }

    @Override
    public byte[] list() {
        File register = fileSystemManager.createRegister();
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
