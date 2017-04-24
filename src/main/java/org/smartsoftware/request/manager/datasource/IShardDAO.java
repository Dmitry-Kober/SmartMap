package org.smartsoftware.request.manager.datasource;

import org.smartsoftware.domain.IKey;

import java.nio.file.Path;
import java.sql.Timestamp;

/**
 * Created by Dmitry on 23.04.2017.
 */
public interface IShardDAO {

    boolean addUncommittedEntry(Timestamp timestamp, IKey key, String metaData);
    boolean commitEntry(Timestamp timestamp, IKey key);

}
