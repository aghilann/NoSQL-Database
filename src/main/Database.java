package main;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.io.*;

public class Database {
    // Constants
    public static final String BUCKET_FILE_NAME = "buckets.dat";
    public static final String DATA_FILE_NAME = "data.dat";
    private static final int BUCKET_SIZE = 8;
    private static final int NUM_BUCKETS = 10000;
    private static final int DELETION_THRESHOLD = 10;
    private static final double LOAD_FACTOR_THRESHOLD_UPPER = 0.7;
    private static final double LOAD_FACTOR_THRESHOLD_LOWER = 0.3;

    // Variables
    private RandomAccessFile bucketFile;
    private RandomAccessFile dataFile;
    private int currentNumBuckets = NUM_BUCKETS;
    private ReentrantReadWriteLock[] bucketLocks = new ReentrantReadWriteLock[currentNumBuckets];
    private final Object dataFileLock = new Object();
    private final AtomicInteger deletionsCounter = new AtomicInteger(0);
    private final ReentrantLock resizeLock = new ReentrantLock();
    private final AtomicInteger usedBucketsCount = new AtomicInteger(0);

    /**
     * Constructor: Initializes the database.
     * Creates or opens the bucket and data files.
     * If the bucket file is empty, pre-allocate space for initial buckets.
     */
    public Database() throws IOException {
        this.bucketFile = new RandomAccessFile(BUCKET_FILE_NAME, "rw");
        this.dataFile = new RandomAccessFile(DATA_FILE_NAME, "rw");

        for (int i = 0; i < currentNumBuckets; i++) {
            bucketLocks[i] = new ReentrantReadWriteLock();
        }

        if (bucketFile.length() == 0) {
            for (int i = 0; i < currentNumBuckets; i++) {
                bucketFile.writeLong(-1L);
            }
        }
    }

    /**
     * Puts a key-value pair in the database.
     * Writes the value to the data file and updates the bucket with a pointer to the data.
     * Checks and resizes the buckets if necessary after writing.
     */
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

                if (shouldResize()) {
                    resize();
                }
            }

            // Update bucket with pointer
            bucketFile.seek(bucketIndex * BUCKET_SIZE);
            bucketFile.writeLong(pointer);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Deletes a key-value pair from the database.
     * Marks the entry in the bucket as deleted and checks for compacting or resizing if needed.
     */
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

            if (shouldResize()) {
                resize();
            }

        } finally {
            lock.writeLock().unlock();
        }

        return true;
    }

    /**
     * Fetches the value associated with a key from the database.
     * Reads the pointer from the bucket and fetches the data from the data file using the pointer.
     */
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

    /**
     * Compacts the data file.
     * Creates a temporary data file, copies over only the valid (non-deleted) entries, then replaces the old file.
     */
    public void compact() throws IOException {
        synchronized(dataFileLock) {
            RandomAccessFile tempDataFile = new RandomAccessFile(DATA_FILE_NAME + ".tmp", "rw");

            for (int i = 0; i < currentNumBuckets; i++) {
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
                        bucketFile.seek((long) i * BUCKET_SIZE);
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

    /**
     * Calculates the bucket index for a given key using its hash code.
     * The resulting index determines where the pointer to the data is stored.
     */
    private long getBucketIndex(@NotNull String key) {
        return Math.abs(key.hashCode()) % currentNumBuckets;
    }

    /**
     * Checks if the bucket file needs resizing based on the load factor.
     * Compares the current load factor to defined upper and lower thresholds.
     */
    private boolean shouldResize() throws IOException {
        double loadFactor = (double) usedBucketsCount.get() / currentNumBuckets;
        return (loadFactor > LOAD_FACTOR_THRESHOLD_UPPER ||
                (loadFactor < LOAD_FACTOR_THRESHOLD_LOWER && currentNumBuckets > NUM_BUCKETS));
    }

    /**
     * Resizes the bucket file based on the current load factor.
     * Depending on the load, either doubles the number of buckets or reduces it back to the initial number.
     */
    private void resize() throws IOException {
        int newNumBuckets = (currentNumBuckets > NUM_BUCKETS &&
                currentNumBuckets * LOAD_FACTOR_THRESHOLD_LOWER < NUM_BUCKETS) ?
                NUM_BUCKETS :
                2 * currentNumBuckets;

        RandomAccessFile newBucketFile = new RandomAccessFile(BUCKET_FILE_NAME + ".tmp", "rw");

        // Preallocate buckets in the new file
        for (int i = 0; i < newNumBuckets; i++) {
            newBucketFile.writeLong(-1L);
        }

        for (int i = 0; i < currentNumBuckets; i++) {
            bucketFile.seek((long) i * BUCKET_SIZE);
            long pointer = bucketFile.readLong();
            if (pointer != -1L) {
                dataFile.seek(pointer);
                int jsonDataLength = dataFile.readInt();
                byte[] jsonData = new byte[jsonDataLength];
                dataFile.read(jsonData);
                String key = new String(jsonData);  // This assumes key can be derived from jsonData
                long newBucketIndex = Math.abs(key.hashCode()) % newNumBuckets;
                newBucketFile.seek(newBucketIndex * BUCKET_SIZE);
                newBucketFile.writeLong(pointer);
            }
        }

        ReentrantReadWriteLock[] newBucketLocks = new ReentrantReadWriteLock[newNumBuckets];
        for (int i = 0; i < newNumBuckets; i++) {
            newBucketLocks[i] = new ReentrantReadWriteLock();
        }
        bucketLocks = newBucketLocks;

        newBucketFile.close();
        bucketFile.close();

        File oldBucketFile = new File(BUCKET_FILE_NAME);
        oldBucketFile.delete();
        File tempBucketFile = new File(BUCKET_FILE_NAME + ".tmp");
        tempBucketFile.renameTo(oldBucketFile);

        bucketFile = new RandomAccessFile(BUCKET_FILE_NAME, "rw");
        currentNumBuckets = newNumBuckets;

        usedBucketsCount.set(newNumBuckets);
    }

    /**
     * Closes both bucket and data files, ensuring all changes are saved.
     */
    public void close() throws IOException {
        bucketFile.close();
        dataFile.close();
    }
}