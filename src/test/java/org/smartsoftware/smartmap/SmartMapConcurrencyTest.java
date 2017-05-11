package org.smartsoftware.smartmap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartsoftware.smartmap.filesystem.FileSystemManager;
import org.smartsoftware.smartmap.filesystem.IFileSystemManager;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

/**
 * Created by dkober on 11.5.2017 г..
 */
public class SmartMapConcurrencyTest {

    private static final Logger LOG = LoggerFactory.getLogger(SmartMapConcurrencyTest.class);

    private ISmartMap smartMap;
    private ScheduledFuture[] futureResults;
    private volatile long terminationTime;

    @Before
    public void setUp() {
        smartMap = null;
        futureResults = null;
        terminationTime = 0;
    }

    @Test
    public void checkPutOverGetPrecedenceSameKey() {
        providedIPreparedSmartMapWithDelayedMethod("createOrReplaceFileWithValue", 10000);
        providedIScheduled(
                new ScheduledCallable<>(() -> {
                    LOG.trace("Putter thread: {}.", Thread.currentThread().getName());
                    smartMap.put("key", "value".getBytes());
                    return true;
                }, 1, TimeUnit.SECONDS),
                new ScheduledCallable<>(() -> {
                    LOG.trace("Getter thread: {}.", Thread.currentThread().getName());
                    smartMap.get("key");
                    return true;
                }, 2, TimeUnit.SECONDS)
            );

        afterIWaitFor(2000);

        whenIReceiveATerminationTimeOfCallable(1);
        itAppearsToBeGreaterThan(5000);
    }

    @Test
    public void shouldCorrectlyResolveConcurrentModificationEventualConsistency() {
        smartMap = new SmartMap();

        Thread thread1 = new Thread(() -> {
            try {
                smartMap.put("thread_key1", "thread_1_value1".getBytes());
                smartMap.put("thread_key2", "thread_1_value2".getBytes());
                smartMap.put("thread_key3", "thread_1_value3".getBytes());
                Thread.sleep(1000);
                smartMap.put("thread_key2", "thread_1_value2_changed_1".getBytes());
                smartMap.get("thread_key2");
                smartMap.put("thread_key3", "thread_1_value3_changed_1".getBytes());
                smartMap.remove("thread_key2");
                Thread.sleep(1000);
                smartMap.get("thread_key1");
                smartMap.get("thread_key2");
                smartMap.get("thread_key3");
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                smartMap.put("thread_key1", "thread_2_value1".getBytes());
                smartMap.put("thread_key2", "thread_2_value2".getBytes());
                smartMap.put("thread_key3", "thread_2_value3".getBytes());
                Thread.sleep(1000);
                smartMap.put("thread_key2", "thread_2_value2_changed_1".getBytes());
                smartMap.get("thread_key2");
                smartMap.put("thread_key3", "thread_2_value3_changed_1".getBytes());
                smartMap.remove("thread_key2");
                Thread.sleep(1000);
                smartMap.get("thread_key1");
                smartMap.get("thread_key2");
                smartMap.get("thread_key3");
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

        assertThat(
                new String(smartMap.get("thread_key1")),
                anyOf(equalTo("thread_1_value1"), equalTo("thread_2_value1"))
        );

        assertThat(
                new String(smartMap.get("thread_key2")),
                equalTo("")
        );

        assertThat(
                new String(smartMap.get("thread_key3")),
                anyOf(equalTo("thread_1_value3_changed_1"), equalTo("thread_2_value3_changed_1"))
        );
    }

    private void afterIWaitFor(long delay) {
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void parallelGetPutDifferentKey() {
        providedIPreparedSmartMapWithDelayedMethod("createOrReplaceFileWithValue", 80000);
        providedIScheduled(
                new ScheduledCallable<>(() -> {
                    LOG.trace("Putter thread: {}.", Thread.currentThread().getName());
                    smartMap.put("key", "value".getBytes());
                    return true;
                }, 1, TimeUnit.SECONDS),
                new ScheduledCallable<>(() -> {
                    LOG.trace("Getter thread: {}.", Thread.currentThread().getName());
                    smartMap.get("other_key");
                    return true;
                }, 2, TimeUnit.SECONDS)
        );

        afterIWaitFor(2000);

        whenIReceiveATerminationTimeOfCallable(1);
        itAppearsToBeLessThan(5000);
    }

    private void itAppearsToBeGreaterThan(long millis) {
        assertTrue(terminationTime > millis);
    }

    private void itAppearsToBeLessThan(long millis) {
        assertTrue(terminationTime < millis);
    }

    private void whenIReceiveATerminationTimeOfCallable(int index) {
        long start = System.currentTimeMillis();
        try {
            futureResults[index].get();
        }
        catch (InterruptedException | ExecutionException e) {
            fail(e.getMessage());
        }
        long end = System.currentTimeMillis();
        terminationTime = end - start;
    }

    private class ScheduledCallable<T> {
        private final Callable<T> callable;
        private final int interval;
        private final TimeUnit timeUnit;

        public ScheduledCallable(Callable<T> callable, int interval, TimeUnit timeUnit) {
            this.callable = callable;
            this.interval = interval;
            this.timeUnit = timeUnit;
        }
    }

    private void providedIScheduled(ScheduledCallable... callables) {
        futureResults = new ScheduledFuture[callables.length];

        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(callables.length);
        for (int i = 0; i < callables.length; i++) {
            ScheduledCallable callable = callables[i];
            futureResults[i] = executor.schedule(callable.callable, callable.interval, callable.timeUnit);
        }
    }

    private void providedIPreparedSmartMapWithDelayedMethod(String methodName, long delay) {
        IFileSystemManager originalFileSystemManger = new FileSystemManager("repository");

        IFileSystemManager delayedPutFileSystemManager = (IFileSystemManager) Proxy.newProxyInstance(IFileSystemManager.class.getClassLoader(), new Class[]{IFileSystemManager.class}, (proxy, method, args) -> {
            LOG.trace("{}: the '{}()' method is about to be invoked by the '{}' thread.", new Object[]{System.currentTimeMillis(),  method.getName(), Thread.currentThread().getName()});
            if (methodName.equals(method.getName())) {
                Thread.sleep(delay);
            }
            Object result = method.invoke(originalFileSystemManger, args);
            LOG.trace("{}: the '{}()' method returned a result to the '{}' thread.", new Object[] {System.currentTimeMillis(), method.getName(), Thread.currentThread().getName()});
            return result;
        });

        smartMap = new SmartMap(delayedPutFileSystemManager);
    }

    @After
    public void tearDown() throws IOException {
        Path workingFolderPath = Paths.get(SmartMap.WORKING_FOLDER);
        Files.list(workingFolderPath).forEach(file -> file.toFile().delete());
    }

}
