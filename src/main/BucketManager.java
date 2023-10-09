package main;

import java.util.concurrent.atomic.AtomicInteger;

public class BucketManager {
    static final int NUM_BUCKETS = 10000;
    public AtomicInteger currentNumBuckets = new AtomicInteger(NUM_BUCKETS);
    public AtomicInteger usedBucketsCount = new AtomicInteger(0);

    public long getBucketIndex(String key) {
        return Math.abs(key.hashCode()) % currentNumBuckets.get();
    }

    public boolean shouldResize(double loadFactorThresholdUpper, double loadFactorThresholdLower) {
        double loadFactor = (double) usedBucketsCount.get() / currentNumBuckets.get();
        return (loadFactor > loadFactorThresholdUpper ||
                (loadFactor < loadFactorThresholdLower && currentNumBuckets.get() > NUM_BUCKETS));
    }

    public void incrementUsedBuckets() {
        usedBucketsCount.incrementAndGet();
    }

    // Assuming you also need a method to decrement, given the delete operation
    public void decrementUsedBuckets() {
        usedBucketsCount.decrementAndGet();
    }
}
