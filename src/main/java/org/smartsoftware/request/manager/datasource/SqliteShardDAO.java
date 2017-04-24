package org.smartsoftware.request.manager.datasource;

import org.smartsoftware.domain.IKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.Timestamp;

/**
 * Created by Dmitry on 23.04.2017.
 */
class SqliteShardDAO extends JdbcDaoSupport implements IShardDAO {

    SqliteShardDAO(DataSource dataSource) {
        setDataSource(dataSource);
    }


    public boolean addUncommittedEntry(Timestamp timestamp, IKey key, String metaData) {
        return false;
    }

    public boolean commitEntry(Timestamp timestamp, IKey key) {
        return false;
    }
}
