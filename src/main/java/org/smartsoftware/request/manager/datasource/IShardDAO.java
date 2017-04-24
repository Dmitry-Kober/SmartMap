package org.smartsoftware.request.manager.datasource;

import org.smartsoftware.domain.data.IKey;

import java.sql.Timestamp;
import java.util.Optional;

/**
 * Created by Dmitry on 23.04.2017.
 */
public interface IShardDAO {

    void init();

    Optional<String> getCommittedPathFor(IKey key);

    boolean addUpdatingEntry(Timestamp timestamp, IKey key, String filePath);
    boolean commitEntry(Timestamp timestamp, IKey key);

}
