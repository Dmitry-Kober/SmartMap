package org.smartsoftware.request.manager.datasource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartsoftware.domain.data.IKey;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.sql.*;
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
            "ORDER BY asAt DESC";

    private static final String CREATE_TABLE_IF_NOT_EXISTS = "CREATE TABLE IF NOT EXISTS ENTRIES(" +
            "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "entry_key TEXT NOT NULL, " +
            "asAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
            "path TEXT NOT NULL, " +
            "status TEXT CHECK(status IN ('UPDATING', 'COMMITTED', )) " +
            ");";

    private static final String ADD_UPDATING_ENTRY = "INSERT INTO ENTRIES (entry_key, asAt, path) VALUES (?, ?, ?, 'UPDATING');";

    private static final String COMMIT_ENTRY = "UPDATE ENTRIES SET status = 'COMMITTED' WHERE entry_key = ? AND timestamp = ? AND status = 'UPDATING';";

    private static final String REMOVE_ALL_UNCOMMITTED_ENTRIES = "DELETE FROM ENTRIES WHERE status != 'COMMITTED';";

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
        String path = getJdbcTemplate().queryForObject(GET_LATEST_COMMITTED_FILE_PATH, new Object[]{key.get()}, String.class);
        return Optional.ofNullable(path);
    }

    public boolean addUpdatingEntry(Timestamp timestamp, IKey key, String filePath) {
        int updatedRecords = getJdbcTemplate().update(ADD_UPDATING_ENTRY, key.get(), timestamp, filePath);
        return updatedRecords > 0;
    }

    public boolean commitEntry(Timestamp timestamp, IKey key) {
        int updatedRecords = getJdbcTemplate().update(COMMIT_ENTRY, key.get(), timestamp);
        return updatedRecords > 0;
    }
}
