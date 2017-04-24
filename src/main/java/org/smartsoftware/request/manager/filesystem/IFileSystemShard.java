package org.smartsoftware.request.manager.filesystem;

import org.smartsoftware.domain.data.IValue;

import java.nio.file.Path;

/**
 * Created by dkober on 24.4.2017 г..
 */
public interface IFileSystemShard {

    void init();

    boolean lockFile(Path path);
    boolean unlockFile(Path path);

    boolean createNewFileWithValue(Path path, IValue value);
    boolean removeAllFilesWithMask(Path shardPath, String fileNameMask);

    IValue getValueFrom(Path path);
}
