package org.smartsoftware.request.manager;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.smartsoftware.domain.communication.CommunicationChain;
import org.smartsoftware.domain.communication.request.GetRequest;
import org.smartsoftware.domain.communication.request.IRequest;
import org.smartsoftware.domain.communication.request.PutRequest;
import org.smartsoftware.domain.communication.request.RemoveRequest;
import org.smartsoftware.domain.communication.response.SuccessResponse;
import org.smartsoftware.domain.communication.response.ValueResponse;
import org.smartsoftware.domain.data.ByteArrayValue;
import org.smartsoftware.domain.data.StringKey;
import org.smartsoftware.request.manager.datasource.SqliteShardDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

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
        List<Integer> result = executeOnDb(
                "SELECT COUNT(*) FROM ENTRIES where status != 'COMMITTED';",
                (resultSet, i) -> resultSet.getInt(1)
        );
        assertThat(result, contains(0));
    }


    @Test
    public void shouldAddOneKeyValuePair() {
        IRequest putRequest = new PutRequest(
                new StringKey("test_key_addition"),
                new ByteArrayValue("test_key_addition".getBytes())
        );

        CommunicationChain communicationChain = requestManager.onRequest(
                new CommunicationChain(putRequest)
        );

        assertThat(communicationChain.getResponse(), allOf(notNullValue(), instanceOf(SuccessResponse.class)));

        List<Integer> numberOfEntriesInDb = executeOnDb("SELECT COUNT(*) FROM ENTRIES;", (resultSet, i) -> resultSet.getInt(1));
        assertThat(numberOfEntriesInDb, hasItem(greaterThanOrEqualTo(1)));
    }

    @Test
    public void shouldGetExistingValue() {
        IRequest putRequest = new PutRequest(
                new StringKey("test_key_get"),
                new ByteArrayValue("test_value_get".getBytes())
        );
        requestManager.onRequest(new CommunicationChain(putRequest));

        IRequest getRequest = new GetRequest(new StringKey("test_key_get"));
        CommunicationChain communicationChain = requestManager.onRequest(new CommunicationChain(getRequest));
        assertThat(
                communicationChain.getResponse(),
                allOf(notNullValue(), instanceOf(ValueResponse.class))
        );
        assertThat(
                new String(
                        ((ValueResponse) communicationChain.getResponse()).getValue().get().orElse("Nan".getBytes())
                ),
                equalTo("test_value_get")
        );
    }

    @Test
    public void shouldRemoveExistingValue() {
        IRequest putRequest = new PutRequest(
                new StringKey("test_key_remove"),
                new ByteArrayValue("test_value_remove".getBytes())
        );
        requestManager.onRequest(new CommunicationChain(putRequest));

        IRequest putRequestOther = new PutRequest(
                new StringKey("test_key_remove_other"),
                new ByteArrayValue("test_value_remove_other".getBytes())
        );
        requestManager.onRequest(new CommunicationChain(putRequestOther));

        IRequest removeRequest = new RemoveRequest(new StringKey("test_key_remove"));
        CommunicationChain communicationChain = requestManager.onRequest(new CommunicationChain(removeRequest));
        assertThat(
                communicationChain.getResponse(),
                allOf(notNullValue(), instanceOf(SuccessResponse.class))
        );

        List<Integer> numberOfEntriesInDbKeyRemove = executeOnDb(
                "SELECT COUNT(*) FROM ENTRIES WHERE entry_key = 'test_key_remove';",
                (resultSet, i) -> resultSet.getInt(1)
        );
        assertThat(numberOfEntriesInDbKeyRemove, hasItem(greaterThanOrEqualTo(0)));

        List<Integer> numberOfEntriesInDbKeyRemoveOther = executeOnDb(
                "SELECT COUNT(*) FROM ENTRIES WHERE entry_key = 'test_key_remove';",
                (resultSet, i) -> resultSet.getInt(1)
        );
        assertThat(numberOfEntriesInDbKeyRemoveOther, hasItem(greaterThanOrEqualTo(1)));
    }

    private <T> List<T> executeOnDb(String query, RowMapper<T> rowMapper) {
        try {
            Field shardsField = HashBasedRequestManager.class.getDeclaredField("shards");
            shardsField.setAccessible(true);

            List<Shard> shards = (List<Shard>) shardsField.get(requestManager);
            if (shards.size() != 1) {
                throw new IllegalStateException("Unexpected shards' configuration.");
            }

            Shard shard = shards.stream().findFirst().get();
            SqliteShardDAO sqliteShardDao = (SqliteShardDAO) shard.getDao();

            Field jdbcTemplateField = sqliteShardDao.getClass().getSuperclass().getDeclaredField("jdbcTemplate");
            jdbcTemplateField.setAccessible(true);

            JdbcTemplate shardJdbcTemplate = (JdbcTemplate) jdbcTemplateField.get(sqliteShardDao);

            return shardJdbcTemplate.query(query, rowMapper);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
