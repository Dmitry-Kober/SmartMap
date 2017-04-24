package org.smartsoftware.request.manager.datasource;

import org.smartsoftware.domain.IKey;

import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.Optional;

/**
 * Created by Dmitry on 23.04.2017.
 */
public interface IShardDAO {

    void init();

    Optional<String> getCommittedPathFor(IKey key);

    boolean addUpdatingEntry(Timestamp timestamp, IKey key, String metaData);
    boolean commitEntry(Timestamp timestamp, IKey key);

}
