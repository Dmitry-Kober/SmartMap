package org.smartsoftware.request.manager;

import org.smartsoftware.request.manager.datasource.IShardDAO;
import org.smartsoftware.request.manager.filesystem.IFileSystemShard;

/**
 * Created by dkober on 24.4.2017 г..
 */
public class Shard {

    private final IShardDAO dao;
    private final IFileSystemShard fileSystem;

    public Shard(IShardDAO dao, IFileSystemShard fileSystem) {
        this.dao = dao;
        this.fileSystem = fileSystem;
    }

    public IShardDAO getDao() {
        return dao;
    }

    public IFileSystemShard getFileSystem() {
        return fileSystem;
    }
}
