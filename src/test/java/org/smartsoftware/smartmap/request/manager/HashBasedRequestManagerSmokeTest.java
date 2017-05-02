package org.smartsoftware.smartmap.request.manager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.smartsoftware.smartmap.domain.communication.CommunicationChain;
import org.smartsoftware.smartmap.domain.communication.request.*;
import org.smartsoftware.smartmap.domain.communication.response.EmptyResponse;
import org.smartsoftware.smartmap.domain.communication.response.ListResponse;
import org.smartsoftware.smartmap.domain.communication.response.SuccessResponse;
import org.smartsoftware.smartmap.domain.communication.response.ValueResponse;
import org.smartsoftware.smartmap.domain.data.ByteArrayValue;
import org.smartsoftware.smartmap.domain.data.StringKey;
import org.smartsoftware.smartmap.request.manager.filesystem.FileSystemShard;
import org.smartsoftware.smartmap.request.manager.filesystem.IFileSystemShard;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by Dmitry on 23.04.2017.
 */
public class HashBasedRequestManagerSmokeTest {

    private HashBasedRequestManager requestManager;

    @Before
    public void setUp() throws IOException {
        IFileSystemShard fileSystemShard = new FileSystemShard("smartmap");
        requestManager = new HashBasedRequestManager(
                Collections.singletonList(
                        new Shard("shard1", fileSystemShard)
                )
        );

        Path shardPath = Paths.get(getShard().getPath());
        Files.list(shardPath).forEach(file -> file.toFile().delete());
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

        Shard shard = getShard();
        Path filePath = Paths.get(shard.getPath() + "/test_key_addition.data");
        assertTrue(Files.exists(filePath));

        Optional<byte[]> value = shard.getFileSystem().getValueFrom(filePath).get();
        assertTrue(value.isPresent() && new String(value.get()).equals("test_key_addition"));
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

        Shard shard = getShard();
        Path removeKeyFilePath = Paths.get(shard.getPath() + "/test_key_remove.data");
        assertFalse(Files.exists(removeKeyFilePath));

        Path otherFilePath = Paths.get(shard.getPath() + "/test_key_remove_other.data");
        assertTrue(Files.exists(otherFilePath));
    }

    @Test
    public void shouldListAllLatestCommittedKeys() {
        requestManager.onRequest(new CommunicationChain(new PutRequest(new StringKey("list_key_1"), new ByteArrayValue("list_value_1".getBytes()))));
        requestManager.onRequest(new CommunicationChain(new PutRequest(new StringKey("list_key_2"), new ByteArrayValue("list_value_2".getBytes()))));

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
                requestManager.onRequest(new CommunicationChain(new PutRequest(new StringKey("thread_key1"), new ByteArrayValue("thread_1_value1".getBytes()))));
                requestManager.onRequest(new CommunicationChain(new PutRequest(new StringKey("thread_key2"), new ByteArrayValue("thread_1_value2".getBytes()))));
                requestManager.onRequest(new CommunicationChain(new PutRequest(new StringKey("thread_key3"), new ByteArrayValue("thread_1_value3".getBytes()))));
                Thread.sleep(1000);
                requestManager.onRequest(new CommunicationChain(new PutRequest(new StringKey("thread_key2"), new ByteArrayValue("thread_1_value2_changed_1".getBytes()))));
                requestManager.onRequest(new CommunicationChain(new GetRequest(new StringKey("thread_key2"))));
                requestManager.onRequest(new CommunicationChain(new PutRequest(new StringKey("thread_key3"), new ByteArrayValue("thread_1_value3_changed_1".getBytes()))));
                requestManager.onRequest(new CommunicationChain(new RemoveRequest(new StringKey("thread_key2"))));
                Thread.sleep(1000);
                requestManager.onRequest(new CommunicationChain(new GetRequest(new StringKey("thread_key1"))));
                requestManager.onRequest(new CommunicationChain(new GetRequest(new StringKey("thread_key2"))));
                requestManager.onRequest(new CommunicationChain(new GetRequest(new StringKey("thread_key3"))));
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                requestManager.onRequest(new CommunicationChain(new PutRequest(new StringKey("thread_key1"), new ByteArrayValue("thread_2_value1".getBytes()))));
                requestManager.onRequest(new CommunicationChain(new PutRequest(new StringKey("thread_key2"), new ByteArrayValue("thread_2_value2".getBytes()))));
                requestManager.onRequest(new CommunicationChain(new PutRequest(new StringKey("thread_key3"), new ByteArrayValue("thread_2_value3".getBytes()))));
                Thread.sleep(1000);
                requestManager.onRequest(new CommunicationChain(new PutRequest(new StringKey("thread_key2"), new ByteArrayValue("thread_2_value2_changed_1".getBytes()))));
                requestManager.onRequest(new CommunicationChain(new GetRequest(new StringKey("thread_key2"))));
                requestManager.onRequest(new CommunicationChain(new PutRequest(new StringKey("thread_key3"), new ByteArrayValue("thread_2_value3_changed_1".getBytes()))));
                requestManager.onRequest(new CommunicationChain(new RemoveRequest(new StringKey("thread_key2"))));
                Thread.sleep(1000);
                requestManager.onRequest(new CommunicationChain(new GetRequest(new StringKey("thread_key1"))));
                requestManager.onRequest(new CommunicationChain(new GetRequest(new StringKey("thread_key2"))));
                requestManager.onRequest(new CommunicationChain(new GetRequest(new StringKey("thread_key3"))));
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

        CommunicationChain key1CommPath = requestManager.onRequest(new CommunicationChain(new GetRequest(new StringKey("thread_key1"))));
        assertThat(
                key1CommPath.getResponse(),
                allOf(notNullValue(), instanceOf(ValueResponse.class))
        );
        assertThat(
                new String(
                        ((ValueResponse) key1CommPath.getResponse()).getValue().get().orElse("Nan".getBytes())
                ),
                anyOf(equalTo("thread_1_value1"), equalTo("thread_2_value1"))
        );

        CommunicationChain key2CommPath = requestManager.onRequest(new CommunicationChain(new GetRequest(new StringKey("thread_key2"))));
        assertThat(
                key2CommPath.getResponse(),
                allOf(notNullValue(), instanceOf(EmptyResponse.class))
        );

        CommunicationChain key3CommPath = requestManager.onRequest(new CommunicationChain(new GetRequest(new StringKey("thread_key3"))));
        assertThat(
                key3CommPath.getResponse(),
                allOf(notNullValue(), instanceOf(ValueResponse.class))
        );
        assertThat(
                new String( ((ValueResponse) key3CommPath.getResponse()).getValue().get().orElse("Nan".getBytes()) ),
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

}
