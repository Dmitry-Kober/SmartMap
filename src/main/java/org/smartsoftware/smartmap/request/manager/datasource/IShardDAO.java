package org.smartsoftware.smartmap.request.manager.datasource;

import org.smartsoftware.smartmap.domain.data.IKey;

import java.sql.Timestamp;
import java.util.*;

/**
 * Created by Dmitry on 23.04.2017.
 */
public interface IShardDAO {

    void init();

    Optional<String> getCommittedPathFor(IKey key);
    List<String> getAllLatestCommittedKeys();

    boolean addUpdatingEntry(Timestamp timestamp, IKey key, String filePath);
    boolean commitEntry(Timestamp timestamp, IKey key);
    boolean markEntriesAsRemoved(IKey key);

    Map<Integer, String> getAllRemovedAndCommittedEntriesButLatest();
    boolean removeEntries(Set<Integer> ids);

}
