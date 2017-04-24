package org.smartsoftware.request.manager;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.smartsoftware.request.manager.datasource.IShardDAO;
import org.smartsoftware.request.manager.datasource.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Created by Dmitry on 23.04.2017.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:test-context.xml"})
public class HashBasedRequestManagerTest {

    @Autowired
    private HashBasedRequestManager requestManager;

    @Test
    public void testFreshInitializationProcess() throws IllegalAccessException, NoSuchFieldException, SQLException {
        Field shardsField = HashBasedRequestManager.class.getDeclaredField("shards");
        shardsField.setAccessible(true);

        Map<String, Shard> shards = (Map<String, Shard>) shardsField.get(requestManager);

        for (Map.Entry<String, Shard> shardEntry : shards.entrySet()) {
            IShardDAO shardDAO = shardEntry.getValue().getDao();

            if (shardDAO instanceof SqliteShardDAO) {
                SqliteShardDAO sqliteShardDao = (SqliteShardDAO) shardDAO;
                Field jdbcTemplateField = sqliteShardDao.getClass().getSuperclass().getDeclaredField("jdbcTemplate");
                jdbcTemplateField.setAccessible(true);

                JdbcTemplate shardJdbcTemplate = (JdbcTemplate) jdbcTemplateField.get(sqliteShardDao);
                List<Integer> result = shardJdbcTemplate.query("SELECT COUNT(*) FROM ENTRIES where status != 'COMMITTED';", new RowMapper<Integer>() {
                    @Override
                    public Integer mapRow(ResultSet resultSet, int i) throws SQLException {
                        return resultSet.getInt(1);
                    }
                });
                assertThat(result, hasSize(1));
                assertThat(result, hasItem(0));
            }
            else {
                fail("Unexpected implementation of the IShardDAO met.");
            }
        }
    }
}
