package org.smartsoftware.smartmap.request.manager.filesystem;

import org.smartsoftware.smartmap.domain.data.IValue;

import java.nio.file.Path;
import java.util.Set;

/**
 * Created by dkober on 24.4.2017 г..
 */
public interface IFileSystemShard {

    boolean createOrReplaceFileWithValue(Path path, IValue value);
    boolean removeFile(Path path);
    Set<String> listAllFilesInShardMatching(Path path, String mask);

    IValue getValueFrom(Path path);
}
