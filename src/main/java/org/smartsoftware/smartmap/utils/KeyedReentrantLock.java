package org.smartsoftware.smartmap.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by dkober on 2.5.2017 г..
 */
public class KeyedReentrantLock<T> {

    private ConcurrentMap<T, ReentrantReadWriteLock> lockMap = new ConcurrentHashMap<>();

    public void writeLock(T key) {
        ReentrantReadWriteLock lock = lockMap.get(key);
        if (lock != null) {
            lock.writeLock().lock();
            return;
        }

        ReentrantReadWriteLock newLock = new ReentrantReadWriteLock();
        newLock.writeLock().lock();
        ReentrantReadWriteLock prevLock = lockMap.putIfAbsent(key, newLock);
        if (prevLock != null) {
            writeLock(key);
        }
    }

    public void readLock(T key) {
        ReentrantReadWriteLock oldLock = lockMap.get(key);
        if (oldLock != null) {
            oldLock.readLock().lock();
            return;
        }

        ReentrantReadWriteLock newLock = new ReentrantReadWriteLock();
        newLock.readLock().lock();
        ReentrantReadWriteLock prevLock = lockMap.putIfAbsent(key, newLock);
        if (prevLock != null) {
            readLock(key);
        }
    }

    public void writeUnlock(T key) {
        ReentrantReadWriteLock lock = lockMap.get(key);
        if (lock == null) {
            throw new IllegalMonitorStateException("There was no lock for the '" + key + "' key.");
        }
        lock.writeLock().unlock();
    }

    public void readUnlock(T key) {
        ReentrantReadWriteLock lock = lockMap.get(key);
        if (lock == null) {
            throw new IllegalMonitorStateException("There was no lock for the '" + key + "' key.");
        }
        lock.readLock().unlock();
    }


}
