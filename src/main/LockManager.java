package main;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {
    private ReentrantReadWriteLock[] bucketLocks;

    public LockManager(int numberOfLocks) {
        bucketLocks = new ReentrantReadWriteLock[numberOfLocks];
        for (int i = 0; i < numberOfLocks; i++) {
            bucketLocks[i] = new ReentrantReadWriteLock();
        }
    }

    public ReentrantReadWriteLock getLock(int index) {
        return bucketLocks[index];
    }
}