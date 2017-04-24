package org.smartsoftware.request.manager.filesystem;

import org.smartsoftware.domain.IValue;

import java.nio.file.Path;

/**
 * Created by dkober on 24.4.2017 г..
 */
public interface IFileSystemShard {

    void init();

    boolean createNewFileWithValue(IValue value);

    IValue getValueFrom(Path filePath);
}
