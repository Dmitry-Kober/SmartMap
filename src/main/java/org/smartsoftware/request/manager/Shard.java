package org.smartsoftware.request.manager;

import org.smartsoftware.request.manager.datasource.IShardDAO;
import org.smartsoftware.request.manager.filesystem.IFileSystemShard;

/**
 * Created by dkober on 24.4.2017 г..
 */
public class Shard {

    private final String path;
    private final IShardDAO dao;
    private final IFileSystemShard fileSystem;

    public Shard(String path, IShardDAO dao, IFileSystemShard fileSystem) {
        this.path = path;
        this.dao = dao;
        this.fileSystem = fileSystem;
    }

    public String getPath() {
        return path;
    }

    public IShardDAO getDao() {
        return dao;
    }

    public IFileSystemShard getFileSystem() {
        return fileSystem;
    }
}
