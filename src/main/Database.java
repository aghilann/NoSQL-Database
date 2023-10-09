package main;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.io.*;

public class Database {
    public static final String BUCKET_FILE_NAME = "buckets.dat";
    public  static final String DATA_FILE_NAME = "data.dat";
    private static final int BUCKET_SIZE = 8; // size of long in bytes, to store file pointers
    private static final int NUM_BUCKETS = 10000; // Setting to a large number to deal with Hash Collisions

    private final RandomAccessFile bucketFile;
    private RandomAccessFile dataFile;
    private final ReentrantReadWriteLock[] bucketLocks = new ReentrantReadWriteLock[NUM_BUCKETS];
    private final Object dataFileLock = new Object();
    private static final int DELETION_THRESHOLD = 10;
    private final AtomicInteger deletionsCounter = new AtomicInteger(0);

    public Database() throws IOException {
        this.bucketFile = new RandomAccessFile(BUCKET_FILE_NAME, "rw");
        this.dataFile = new RandomAccessFile(DATA_FILE_NAME, "rw");

        // Initialize the locks for each bucket
        for (int i = 0; i < NUM_BUCKETS; i++) {
            bucketLocks[i] = new ReentrantReadWriteLock();
        }

        // Pre-allocate buckets if the bucket file is empty
        if (bucketFile.length() == 0) {
            for (int i = 0; i < NUM_BUCKETS; i++) {
                bucketFile.writeLong(-1L); // Initialize with invalid pointers
            }
        }
    }

    public void put(String key, @NotNull String jsonValue) throws IOException {
        long bucketIndex = getBucketIndex(key);
        ReentrantReadWriteLock lock = bucketLocks[(int) bucketIndex];
        lock.writeLock().lock();
        try {
            long pointer = dataFile.length(); // append to end of file

            synchronized(dataFileLock) {
                pointer = dataFile.length(); // append to end of file

                // Write JSON value to data file
                byte[] jsonData = jsonValue.getBytes();
                dataFile.seek(dataFile.length());
                dataFile.writeInt(jsonData.length); // store length of the JSON data
                dataFile.write(jsonData);
            }

            // Update bucket with pointer
            bucketFile.seek(bucketIndex * BUCKET_SIZE);
            bucketFile.writeLong(pointer);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Boolean delete(String key) throws IOException {
        long bucketIndex = getBucketIndex(key);
        ReentrantReadWriteLock lock = bucketLocks[(int) bucketIndex];
        lock.writeLock().lock();
        try {
            bucketFile.seek(bucketIndex * BUCKET_SIZE);
            long pointerPosition = bucketFile.getFilePointer();
            long pointer = bucketFile.readLong();
            // Couldn't delete the item
            if (pointer == -1L) {
                return false;
            }

            bucketFile.seek(pointerPosition);
            bucketFile.writeLong(-1L);

            if (deletionsCounter.incrementAndGet() >= DELETION_THRESHOLD) {
                compact();
                deletionsCounter.set(0); // Reset the counter
            }

        } finally {
            lock.writeLock().unlock();
        }

        return true;
    }

    public String get(String key) throws IOException {
        long bucketIndex = getBucketIndex(key);
        ReentrantReadWriteLock lock = bucketLocks[(int) bucketIndex];
        lock.readLock().lock();
        try {
            // Read pointer from bucket
            bucketFile.seek(bucketIndex * BUCKET_SIZE);
            long pointer = bucketFile.readLong();

            if (pointer == -1L) {
                throw new IOException("Key not found");
            }
            // Read JSON value from data file
            dataFile.seek(pointer);
            int jsonDataLength = dataFile.readInt();
            byte[] jsonData = new byte[jsonDataLength];
            dataFile.read(jsonData);

            return new String(jsonData);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void compact() throws IOException {
        synchronized(dataFileLock) {
            RandomAccessFile tempDataFile = new RandomAccessFile(DATA_FILE_NAME + ".tmp", "rw");

            for (int i = 0; i < NUM_BUCKETS; i++) {
                bucketLocks[i].writeLock().lock();
                try {
                    // Read pointer from the current bucket
                    bucketFile.seek(i * BUCKET_SIZE);
                    long pointer = bucketFile.readLong();

                    if (pointer != -1L) {
                        // Read data associated with pointer from old data file
                        dataFile.seek(pointer);
                        int jsonDataLength = dataFile.readInt();
                        byte[] jsonData = new byte[jsonDataLength];
                        dataFile.read(jsonData);

                        // Write the data to the new (temp) data file
                        long newPointer = tempDataFile.length();
                        tempDataFile.writeInt(jsonDataLength);
                        tempDataFile.write(jsonData);

                        // Update the bucket to point to the new location
                        bucketFile.seek(i * BUCKET_SIZE);
                        bucketFile.writeLong(newPointer);
                    }
                } finally {
                    bucketLocks[i].writeLock().unlock();
                }
            }

            tempDataFile.close();
            dataFile.close();

            // Rename files to switch the compacted file with the old one
            File oldDataFile = new File(DATA_FILE_NAME);
            oldDataFile.delete();
            File newDataFile = new File(DATA_FILE_NAME + ".tmp");
            newDataFile.renameTo(oldDataFile);

            // Re-open the data file
            dataFile = new RandomAccessFile(DATA_FILE_NAME, "rw");
        }
    }


    private long getBucketIndex(@NotNull String key) throws IOException {
        // This is a simple modulo-based hash function. In a real-world scenario, you'd want a more robust hash function.
        return Math.abs(key.hashCode()) % (bucketFile.length() / BUCKET_SIZE);
    }

    public void close() throws IOException {
        bucketFile.close();
        dataFile.close();
    }
}