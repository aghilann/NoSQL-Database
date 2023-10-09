package test;

import main.LRUCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LRUCacheTest {

    private LRUCache<String, String> cache;

    @BeforeEach
    public void setUp() {
        cache = new LRUCache<>(3); // for simplicity, setting maxSize to 3
    }

    @Test
    public void testAddAndGet() {
        cache.setValue("key1", "value1");
        assertEquals("value1", cache.getValue("key1"));
    }

    @Test
    public void testEviction() {
        cache.setValue("key1", "value1");
        cache.setValue("key2", "value2");
        cache.setValue("key3", "value3");

        // All keys should be present
        assertEquals("value1", cache.getValue("key1"));
        assertEquals("value2", cache.getValue("key2"));
        assertEquals("value3", cache.getValue("key3"));

        // Adding a fourth value should evict the first one (key1) because our maxSize is 3
        cache.setValue("key4", "value4");

        assertNull(cache.getValue("key1")); // key1 should be evicted
        assertEquals("value2", cache.getValue("key2"));
        assertEquals("value3", cache.getValue("key3"));
        assertEquals("value4", cache.getValue("key4"));
    }

    @Test
    public void testEvictionWithGet() {
        cache.setValue("key1", "value1");
        cache.setValue("key2", "value2");
        cache.setValue("key3", "value3");

        // Accessing key1 to make it recently used
        cache.getValue("key1");

        // Adding a fourth value should evict the key2 because it's the least recently used
        cache.setValue("key4", "value4");

        assertEquals("value1", cache.getValue("key1"));
        assertNull(cache.getValue("key2")); // key2 should be evicted
        assertEquals("value3", cache.getValue("key3"));
        assertEquals("value4", cache.getValue("key4"));
    }

    // You can add more tests like trying with different object types, checking behavior under concurrent access, etc.
}
