package org.smartsoftware.smartmap.request.manager.filesystem;

import java.io.File;
import java.nio.file.Path;
import java.util.Set;

/**
 * Created by dkober on 24.4.2017 г..
 */
public interface IFileSystemShard {

    void restore();

    boolean createOrReplaceFileWithValue(Path path, byte[] value);
    boolean removeFile(Path path);
    File createShardRegister(Path path);

    byte[] getValueFrom(Path path);
}
