package org.smartsoftware.smartmap.filesystem;

import java.io.File;
import java.nio.file.Path;

/**
 * Created by dkober on 24.4.2017 г..
 */
public interface IFileSystemManager {

    void restore();

    boolean createOrReplaceFileWithValue(Path path, byte[] value);
    boolean removeFile(Path path);
    File createRegister(Path path);

    byte[] getValueFrom(Path path);
}
