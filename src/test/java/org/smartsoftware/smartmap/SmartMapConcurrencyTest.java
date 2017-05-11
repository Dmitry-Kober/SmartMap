package org.smartsoftware.smartmap;

import org.junit.Test;
import org.smartsoftware.smartmap.filesystem.FileSystemManager;
import org.smartsoftware.smartmap.filesystem.IFileSystemManager;

import java.lang.reflect.Proxy;
import java.util.concurrent.*;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by dkober on 11.5.2017 г..
 */
public class SmartMapConcurrencyTest {

    private ISmartMap smartMap;
    private ScheduledFuture[] futureResults;
    private long terminationTime;

    @Test
    public void checkPutOverGetPrecedence() {
        providedIPreparedSmartMapWithDelayedPut();
        providedIScheduled(
                new ScheduledCallable<>(() -> {
                    System.out.println("Putter thread: " + Thread.currentThread().getName());
                    smartMap.put("key", "value".getBytes());
                    return true;
                }, 1, TimeUnit.SECONDS),
                new ScheduledCallable<>(() -> {
                    System.out.println("Getter thread: " + Thread.currentThread().getName());
                    smartMap.get("key");
                    return true;
                }, 2, TimeUnit.SECONDS)
            );

        whenIReceiveATerminationTimeOfCallable(1);
        itAppearsToBeNotLessThan(2000);
    }

    private void itAppearsToBeNotLessThan(long millis) {
        assertTrue(terminationTime > millis);
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

    private void providedIPreparedSmartMapWithDelayedPut() {
        IFileSystemManager originalFileSystemManger = new FileSystemManager("repository");

        IFileSystemManager delayedPutFileSystemManager = (IFileSystemManager) Proxy.newProxyInstance(IFileSystemManager.class.getClassLoader(), new Class[]{IFileSystemManager.class}, (proxy, method, args) -> {
            System.out.println(System.currentTimeMillis() + ": the '" + method.getName() + "()' method is about to be invoked by the '" + Thread.currentThread().getName() + "' thread.");
            if ("createOrReplaceFileWithValue".equals(method.getName())) {
                Thread.sleep(10000);
            }
            Object result = method.invoke(originalFileSystemManger, args);
            System.out.println(System.currentTimeMillis() + ": the '" + method.getName() + "()' method returned a result to the '" + Thread.currentThread().getName() + "' thread.");
            return result;
        });

        smartMap = new SmartMap(delayedPutFileSystemManager);
    }

}
