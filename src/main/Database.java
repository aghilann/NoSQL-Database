package main;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {
    // Constants
    public static final String BUCKET_FILE_NAME = "buckets.dat";
    public static final String DATA_FILE_NAME = "data.dat";
    private static final int BUCKET_SIZE = 8;
    private static final int DELETION_THRESHOLD = 10;
    private static final double LOAD_FACTOR_THRESHOLD_UPPER = 0.7;
    private static final double LOAD_FACTOR_THRESHOLD_LOWER = 0.3;

    // Managers
    public FileManager bucketFileManager;
    public FileManager dataFileManager;
    public BucketManager bucketManager;
    public LockManager lockManager;

    private final Object dataFileLock = new Object();
    private final AtomicInteger deletionsCounter = new AtomicInteger(0);
    private final ReentrantLock resizeLock = new ReentrantLock();

    public Database() throws IOException {
        this.bucketFileManager = new FileManager(BUCKET_FILE_NAME, "rw");
        this.dataFileManager = new FileManager(DATA_FILE_NAME, "rw");
        this.bucketManager = new BucketManager();
        this.lockManager = new LockManager(BucketManager.NUM_BUCKETS);

        if (bucketFileManager.getFileLength() == 0) {
            for (int i = 0; i < BucketManager.NUM_BUCKETS; i++) {
                bucketFileManager.writeLong(-1L);
            }
        }
    }

    public void put(String key, @NotNull String jsonValue) throws IOException {
        long bucketIndex = bucketManager.getBucketIndex(key);
        ReentrantReadWriteLock lock = lockManager.getLock((int) bucketIndex);
        lock.writeLock().lock();
        try {
            long dataFilePointer = -1L; // Default to append to end of file

            synchronized(dataFileLock) {
                dataFilePointer = dataFileManager.getFileLength();
                // Write JSON value to data file
                byte[] jsonData = jsonValue.getBytes();
                dataFileManager.seek(dataFilePointer);
                dataFileManager.writeInt(jsonData.length);
                dataFileManager.write(jsonData);
                if (shouldResize()) {
                    resize();
                }
            }

            // Update bucket with the new pointer (or overwrite old pointer)
            bucketFileManager.writePointerAt(bucketIndex * BUCKET_SIZE, dataFilePointer);

        } finally {
            lock.writeLock().unlock();
        }
    }

    public Boolean delete(String key) throws IOException {
        long bucketIndex = bucketManager.getBucketIndex(key);
        ReentrantReadWriteLock lock = lockManager.getLock((int) bucketIndex);
        lock.writeLock().lock();
        try {
            bucketFileManager.seek(bucketIndex * BUCKET_SIZE);
            long positionPointer = bucketFileManager.getFilePointer();
            long pointer = bucketFileManager.readLong();
            // Couldn't delete the item
            if (pointer == -1L) {
                return false;
            }

            bucketFileManager.writePointerAt(positionPointer, -1L);

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

    public String get(String key) throws IOException {
        long bucketIndex = bucketManager.getBucketIndex(key);
        ReentrantReadWriteLock lock = lockManager.getLock((int) bucketIndex);
        lock.readLock().lock();
        try {
            // Read pointer from bucket
            long pointer = bucketFileManager.readPointerAt(bucketIndex * BUCKET_SIZE);

            if (pointer == -1L) {
                throw new IOException("Key not found");
            }

            // Read JSON value from data file
            int jsonDataLength = dataFileManager.readIntAt(pointer);
            byte[] jsonData = new byte[jsonDataLength];
            dataFileManager.read(jsonData);
            return new String(jsonData);

        } finally {
            lock.readLock().unlock();
        }
    }

    public void compact() throws IOException {
        synchronized(dataFileLock) {
            RandomAccessFile tempDataFile = new RandomAccessFile(DATA_FILE_NAME + ".tmp", "rw");

            for (int i = 0; i < bucketManager.currentNumBuckets.get(); i++) {
                lockManager.getLock(i).writeLock().lock();
                try {
                    // Read pointer from the current bucket
                    long pointer = bucketFileManager.readPointerAt((long) i * BUCKET_SIZE); // Maybe error

                    if (pointer != -1L) {
                        // Read data associated with pointer from old data file
                        int jsonDataLength = dataFileManager.readIntAt(pointer);
                        byte[] jsonData = new byte[jsonDataLength];
                        dataFileManager.read(jsonData);

                        // Write the data to the new (temp) data file
                        long newPointer = tempDataFile.length();
                        tempDataFile.writeInt(jsonDataLength);
                        tempDataFile.write(jsonData);

                        bucketFileManager.writePointerAt((long) i * BUCKET_SIZE, newPointer);
                    }
                } finally {
                    lockManager.getLock(i).writeLock().unlock();
                }
            }

            tempDataFile.close();
            dataFileManager.close();

            File oldDataFile = new File(DATA_FILE_NAME);
            oldDataFile.delete();
            File newDataFile = new File(DATA_FILE_NAME + ".tmp");
            newDataFile.renameTo(oldDataFile);

            // Re-open the data file
            dataFileManager.setFile(new RandomAccessFile(DATA_FILE_NAME, "rw"));
        }
    }

    public void close() throws IOException {
        bucketFileManager.close();
        dataFileManager.close();
    }

    public boolean shouldResize() throws IOException {
        double loadFactor = (double) bucketManager.usedBucketsCount.get() / bucketManager.currentNumBuckets.get();
        return (loadFactor > LOAD_FACTOR_THRESHOLD_UPPER ||
                (loadFactor < LOAD_FACTOR_THRESHOLD_LOWER && bucketManager.currentNumBuckets.get() > BucketManager.NUM_BUCKETS));
    }

    private synchronized void resize() throws IOException {
        int currentNumBuckets = bucketManager.currentNumBuckets.get();
        int baseNumBuckets = bucketManager.NUM_BUCKETS;

        int newNumBuckets = (currentNumBuckets > baseNumBuckets &&
                currentNumBuckets * LOAD_FACTOR_THRESHOLD_LOWER < baseNumBuckets) ?
                baseNumBuckets :
                2 * currentNumBuckets;

        FileManager newBucketFileManager = new FileManager(BUCKET_FILE_NAME + ".tmp", "rw");

        // Preallocate buckets in the new file
        for (int i = 0; i < newNumBuckets; i++) {
            newBucketFileManager.writeLong(-1L);
        }

        for (int i = 0; i < currentNumBuckets; i++) {
            long pointer = bucketFileManager.readPointerAt(i);
            if (pointer != -1L) {
                int jsonDataLength = dataFileManager.readIntAt(pointer);
                byte[] jsonData = new byte[jsonDataLength];
                dataFileManager.read(jsonData);

                // Assumption: Key can be derived from jsonData or you need another way to find the key
                String key = new String(jsonData);  // This needs to be refined
                long newBucketIndex = Math.abs(key.hashCode()) % newNumBuckets;
                newBucketFileManager.writePointerAt(newBucketIndex, pointer);
            }
        }

        // Close and replace the old bucket file
        newBucketFileManager.close();
        bucketFileManager.close();

        File oldBucketFile = new File(BUCKET_FILE_NAME);
        oldBucketFile.delete();
        File tempBucketFile = new File(BUCKET_FILE_NAME + ".tmp");
        tempBucketFile.renameTo(oldBucketFile);

        bucketFileManager = new FileManager(BUCKET_FILE_NAME, "rw");
        bucketManager.currentNumBuckets.set(newNumBuckets);

        // If there's a mechanism to count used buckets, update that too
        bucketManager.usedBucketsCount.set(newNumBuckets);
    }
}