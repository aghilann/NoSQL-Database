package main;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {

    private final int maxSize;

    public LRUCache(int maxSize) {
        // The third argument, true, means that LinkedHashMap should be
        // kept in access order, which is the requirement of LRU cache.
        super(maxSize, 0.75f, true);
        this.maxSize = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        // Remove the oldest item when size exceeds the maximum allowed
        return size() > maxSize;
    }

    // Optionally, if you want to expose methods to manually add or retrieve values:
    public V getValue(K key) {
        return super.get(key);
    }

    public void setValue(K key, V value) {
        super.put(key, value);
    }
}
