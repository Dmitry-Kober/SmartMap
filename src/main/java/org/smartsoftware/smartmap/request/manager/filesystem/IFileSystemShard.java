package org.smartsoftware.smartmap.request.manager.filesystem;

import org.smartsoftware.smartmap.domain.data.IValue;

import java.nio.file.Path;

/**
 * Created by dkober on 24.4.2017 г..
 */
public interface IFileSystemShard {

    void init();

    boolean createNewFileWithValue(Path path, IValue value);
    boolean removeFile(Path path);
    boolean removeAllFilesWithMask(Path shardPath, String fileNameMask);

    IValue getValueFrom(Path path);
}
