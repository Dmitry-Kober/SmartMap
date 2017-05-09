package org.smartsoftware.smartmap.request.manager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.smartsoftware.smartmap.domain.communication.CommunicationChain;
import org.smartsoftware.smartmap.domain.communication.request.*;
import org.smartsoftware.smartmap.domain.communication.response.EmptyResponse;
import org.smartsoftware.smartmap.domain.communication.response.ListResponse;
import org.smartsoftware.smartmap.domain.communication.response.SuccessResponse;
import org.smartsoftware.smartmap.domain.communication.response.ValueResponse;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by Dmitry on 23.04.2017.
 */
public class HashBasedRequestManagerSmokeTest {

    public static final String SHARD_LOCATION = "shard1";
    private HashBasedRequestManager requestManager;

    @Before
    public void setUp() throws IOException {
        requestManager = new HashBasedRequestManager(Collections.singletonList(new Shard(SHARD_LOCATION)));

        Path shardPath = Paths.get(getShard().getPath());
        Files.list(shardPath).forEach(file -> file.toFile().delete());
    }

    @Test
    public void shouldAddOneKeyValuePair() {
        IRequest putRequest = new PutRequest(
                "test_key_addition",
                "test_key_addition".getBytes()
        );

        CommunicationChain communicationChain = requestManager.onRequest(
                new CommunicationChain(putRequest)
        );

        assertThat(communicationChain.getResponse(), allOf(notNullValue(), instanceOf(SuccessResponse.class)));

        Shard shard = getShard();
        Path filePath = Paths.get(SHARD_LOCATION + "/test_key_addition.data");
        assertTrue(Files.exists(filePath));

        byte[] value = shard.getFileSystem().getValueFrom(filePath);
        assertTrue(new String(value).equals("test_key_addition"));
    }

    @Test
    public void shouldGetExistingValue() {
        IRequest putRequest = new PutRequest(
                "test_key_get",
                "test_value_get".getBytes()
        );
        requestManager.onRequest(new CommunicationChain(putRequest));

        IRequest getRequest = new GetRequest("test_key_get");
        CommunicationChain communicationChain = requestManager.onRequest(new CommunicationChain(getRequest));
        assertThat(
                communicationChain.getResponse(),
                allOf(notNullValue(), instanceOf(ValueResponse.class))
        );
        assertThat(
                new String(
                        ((ValueResponse) communicationChain.getResponse()).getValue()
                ),
                equalTo("test_value_get")
        );
    }

    @Test
    public void shouldRemoveExistingValue() {
        IRequest putRequest = new PutRequest(
                "test_key_remove",
                "test_value_remove".getBytes()
        );
        requestManager.onRequest(new CommunicationChain(putRequest));

        IRequest putRequestOther = new PutRequest(
                "test_key_remove_other",
                "test_value_remove_other".getBytes()
        );
        requestManager.onRequest(new CommunicationChain(putRequestOther));

        IRequest removeRequest = new RemoveRequest("test_key_remove");
        CommunicationChain communicationChain = requestManager.onRequest(new CommunicationChain(removeRequest));
        assertThat(
                communicationChain.getResponse(),
                allOf(notNullValue(), instanceOf(SuccessResponse.class))
        );

        Path removeKeyFilePath = Paths.get(SHARD_LOCATION + "/test_key_remove.data");
        assertFalse(Files.exists(removeKeyFilePath));

        Path otherFilePath = Paths.get(SHARD_LOCATION + "/test_key_remove_other.data");
        assertTrue(Files.exists(otherFilePath));
    }

    @Test
    public void shouldListAllLatestCommittedKeys() {
        requestManager.onRequest(new CommunicationChain(new PutRequest("list_key_1", "list_value_1".getBytes())));
        requestManager.onRequest(new CommunicationChain(new PutRequest("list_key_2", "list_value_2".getBytes())));

        IRequest listRequest = new ListKeysRequest();
        CommunicationChain communicationChain = requestManager.onRequest(new CommunicationChain(listRequest));
        assertThat(
                communicationChain.getResponse(),
                allOf(notNullValue(), instanceOf(ListResponse.class) )
        );
        assertThat(((ListResponse)communicationChain.getResponse()).get(), allOf(hasItem("list_key_2"), hasItem("list_key_1")));
    }

    @Test
    public void shouldCorrectlyResolveConcurrentModificationEventualConsistency() {
        Thread thread1 = new Thread(() -> {
            try {
                requestManager.onRequest(new CommunicationChain(new PutRequest("thread_key1", "thread_1_value1".getBytes())));
                requestManager.onRequest(new CommunicationChain(new PutRequest("thread_key2", "thread_1_value2".getBytes())));
                requestManager.onRequest(new CommunicationChain(new PutRequest("thread_key3", "thread_1_value3".getBytes())));
                Thread.sleep(1000);
                requestManager.onRequest(new CommunicationChain(new PutRequest("thread_key2", "thread_1_value2_changed_1".getBytes())));
                requestManager.onRequest(new CommunicationChain(new GetRequest("thread_key2")));
                requestManager.onRequest(new CommunicationChain(new PutRequest("thread_key3", "thread_1_value3_changed_1".getBytes())));
                requestManager.onRequest(new CommunicationChain(new RemoveRequest("thread_key2")));
                Thread.sleep(1000);
                requestManager.onRequest(new CommunicationChain(new GetRequest("thread_key1")));
                requestManager.onRequest(new CommunicationChain(new GetRequest("thread_key2")));
                requestManager.onRequest(new CommunicationChain(new GetRequest("thread_key3")));
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                requestManager.onRequest(new CommunicationChain(new PutRequest("thread_key1", "thread_2_value1".getBytes())));
                requestManager.onRequest(new CommunicationChain(new PutRequest("thread_key2", "thread_2_value2".getBytes())));
                requestManager.onRequest(new CommunicationChain(new PutRequest("thread_key3", "thread_2_value3".getBytes())));
                Thread.sleep(1000);
                requestManager.onRequest(new CommunicationChain(new PutRequest("thread_key2", "thread_2_value2_changed_1".getBytes())));
                requestManager.onRequest(new CommunicationChain(new GetRequest("thread_key2")));
                requestManager.onRequest(new CommunicationChain(new PutRequest("thread_key3", "thread_2_value3_changed_1".getBytes())));
                requestManager.onRequest(new CommunicationChain(new RemoveRequest("thread_key2")));
                Thread.sleep(1000);
                requestManager.onRequest(new CommunicationChain(new GetRequest("thread_key1")));
                requestManager.onRequest(new CommunicationChain(new GetRequest("thread_key2")));
                requestManager.onRequest(new CommunicationChain(new GetRequest("thread_key3")));
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        thread1.start();
        thread2.start();

        try {
            thread1.join();
            thread2.join();
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        CommunicationChain key1CommPath = requestManager.onRequest(new CommunicationChain(new GetRequest("thread_key1")));
        assertThat(
                key1CommPath.getResponse(),
                allOf(notNullValue(), instanceOf(ValueResponse.class))
        );
        assertThat(
                new String(
                        ((ValueResponse) key1CommPath.getResponse()).getValue()
                ),
                anyOf(equalTo("thread_1_value1"), equalTo("thread_2_value1"))
        );

        CommunicationChain key2CommPath = requestManager.onRequest(new CommunicationChain(new GetRequest("thread_key2")));
        assertThat(
                key2CommPath.getResponse(),
                allOf(notNullValue(), instanceOf(EmptyResponse.class))
        );

        CommunicationChain key3CommPath = requestManager.onRequest(new CommunicationChain(new GetRequest("thread_key3")));
        assertThat(
                key3CommPath.getResponse(),
                allOf(notNullValue(), instanceOf(ValueResponse.class))
        );
        assertThat(
                new String( ((ValueResponse) key3CommPath.getResponse()).getValue() ),
                anyOf(equalTo("thread_1_value3_changed_1"), equalTo("thread_2_value3_changed_1"))
        );
    }

    private Shard getShard() {
        try {
            Field shardsField = HashBasedRequestManager.class.getDeclaredField("shards");
            shardsField.setAccessible(true);

            List<Shard> shards = (List<Shard>) shardsField.get(requestManager);
            if (shards.size() != 1) {
                throw new IllegalStateException("Unexpected shards' configuration.");
            }

            return shards.stream().findFirst().get();
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() throws IOException {
        Path shardPath = Paths.get(getShard().getPath());
        Files.list(shardPath).forEach(file -> file.toFile().delete());
    }

    @AfterClass
    public static void deleteFolder() throws IOException {
        Files.deleteIfExists(Paths.get(SHARD_LOCATION));
    }

}
