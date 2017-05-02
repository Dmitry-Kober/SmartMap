package org.smartsoftware.smartmap.request.manager;

import org.smartsoftware.smartmap.request.manager.filesystem.FileSystemShard;
import org.smartsoftware.smartmap.request.manager.filesystem.IFileSystemShard;

/**
 * Created by dkober on 24.4.2017 г..
 */
public class Shard {

    private final String path;
    private final IFileSystemShard fileSystem;

    public Shard(String path) {
        this.path = path;
        this.fileSystem = new FileSystemShard(path);
    }

    public String getPath() {
        return path;
    }

    public IFileSystemShard getFileSystem() {
        return fileSystem;
    }
}
