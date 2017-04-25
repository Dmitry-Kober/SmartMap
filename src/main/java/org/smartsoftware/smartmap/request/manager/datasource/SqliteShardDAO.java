package org.smartsoftware.smartmap.request.manager.datasource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartsoftware.smartmap.domain.data.IKey;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.sql.*;
import java.util.List;
import java.util.Optional;

/**
 * Created by Dmitry on 23.04.2017.
 */
public class SqliteShardDAO extends JdbcDaoSupport implements IShardDAO {

    private static final Logger LOG = LoggerFactory.getLogger(SqliteShardDAO.class);

    private static final String GET_LATEST_COMMITTED_FILE_PATH =
            "SELECT path " +
            "FROM ENTRIES " +
            "WHERE entry_key = ? AND status = 'COMMITTED'" +
            "ORDER BY asAt DESC " +
            "LIMIT 1";

    private static final String GET_ALL_LATEST_COMMITTED_KEYS =
            "SELECT entry_key, MAX(asAt) asAt " +
            "FROM ENTRIES " +
            "WHERE status = 'COMMITTED' " +
            "GROUP BY entry_key ";

    private static final String CREATE_TABLE_IF_NOT_EXISTS = "CREATE TABLE IF NOT EXISTS ENTRIES(" +
            "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "entry_key TEXT NOT NULL, " +
            "asAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
            "path TEXT NOT NULL, " +
            "status TEXT CHECK(status IN ('UPDATING', 'COMMITTED', 'REMOVED')) " +
            ");";

    private static final String ADD_UPDATING_ENTRY = "INSERT INTO ENTRIES (entry_key, asAt, path, status) VALUES (?, ?, ?, 'UPDATING');";

    private static final String COMMIT_ENTRY = "UPDATE ENTRIES SET status = 'COMMITTED' WHERE entry_key = ? AND asAt = ? AND status = 'UPDATING';";

    private static final String REMOVE_ALL_UNCOMMITTED_ENTRIES = "DELETE FROM ENTRIES WHERE status != 'COMMITTED';";

    private static final String MARK_ENTRIES_AS_REMOVED = "UPDATE ENTRIES SET status = 'REMOVED' WHERE entry_key = ?;";

    private Path shardPath;

    SqliteShardDAO(Path shardPath, DataSource dataSource) {
        this.shardPath = shardPath;
        setDataSource(dataSource);
    }

    @Override
    public void init() {
        String shardPathDir = shardPath.toFile().getName();
        LOG.trace("Initializing a DataSource for the : {} shard...", shardPathDir);

        try (Connection shardConnection = getConnection()) {
            // initialize and cleanup the ENTRIES database if required
            boolean isConnectionValid = shardConnection.isValid(1000);
            if ( !isConnectionValid ) {
                throw new RuntimeException("Cannot validate a connection to the " + shardPathDir + " shard.");
            }

            Statement shardInitStmt = shardConnection.createStatement();
            shardInitStmt.execute(CREATE_TABLE_IF_NOT_EXISTS);
            shardInitStmt.execute(REMOVE_ALL_UNCOMMITTED_ENTRIES);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    SqliteShardDAO(DataSource dataSource) {
        setDataSource(dataSource);
    }


    @Override
    public Optional<String> getCommittedPathFor(IKey key) {
        List<String> paths = getJdbcTemplate().query(GET_LATEST_COMMITTED_FILE_PATH, new Object[]{key.get()}, (resultSet, i) -> resultSet.getString(1));
        if (paths.size() == 0) {
            return Optional.empty();
        }
        if (paths.size() == 1) {
            return Optional.of(paths.get(0));
        }
        LOG.error("Unexpected number of paths found for the '{}' key.", key.get());
        return Optional.empty();
    }

    @Override
    public List<String> getAllLatestCommittedKeys() {
        return getJdbcTemplate().query(GET_ALL_LATEST_COMMITTED_KEYS, (resultSet, i) -> resultSet.getString(1));
    }

    public boolean addUpdatingEntry(Timestamp timestamp, IKey key, String filePath) {
        int updatedRecords = getJdbcTemplate().update(ADD_UPDATING_ENTRY, key.get(), timestamp, filePath);
        return updatedRecords > 0;
    }

    public boolean commitEntry(Timestamp timestamp, IKey key) {
        int updatedRecords = getJdbcTemplate().update(COMMIT_ENTRY, key.get(), timestamp);
        return updatedRecords > 0;
    }

    @Override
    public boolean markEntriesAsRemoved(IKey key) {
        int updatedRecords = getJdbcTemplate().update(MARK_ENTRIES_AS_REMOVED, key.get());
        return updatedRecords > 0;
    }
}
