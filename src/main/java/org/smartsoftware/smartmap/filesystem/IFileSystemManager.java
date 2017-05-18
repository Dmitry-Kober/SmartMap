package org.smartsoftware.smartmap.filesystem;

import java.io.File;

/**
 * Created by dkober on 24.4.2017 г..
 */
public interface IFileSystemManager {

    void restore();

    boolean createOrReplaceFileFor(String key, byte[] value);
    boolean removeFileFor(String key);
    File createRegister();
    byte[] getValueFor(String key);
    String getWorkingFolder();
}
