package org.smartsoftware.smartmap.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by dkober on 2.5.2017 г..
 */
public class KeyedReentrantLock<T> {

    private ConcurrentMap<T, Lock> lockMap = new ConcurrentHashMap<>();

    public void writeLock(T key) {
        Lock lock = lockMap.get(key);
        if (lock != null) {
            lock.lock();
            return;
        }

        Lock newLock = new ReentrantLock();
        newLock.lock();
        Lock prevLock = lockMap.putIfAbsent(key, newLock);
        if (prevLock != null) {
            writeLock(key);
        }
    }

    public void writeUnlock(T key) {
        Lock lock = lockMap.get(key);
        if (lock == null) {
            throw new IllegalMonitorStateException("There was no lock for the '" + key + "' key.");
        }
        lock.unlock();
    }


}
