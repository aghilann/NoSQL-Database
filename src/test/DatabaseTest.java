package test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import main.Database;

import static org.junit.jupiter.api.Assertions.*;

public class DatabaseTest {

    private Database db;

    @BeforeEach
    public void setUp() throws IOException {
        db = new Database(true);
    }

    @AfterEach
    public void tearDown() throws IOException {
        db.close();
        // Delete the files after each test
        if (!new File(Database.BUCKET_FILE_NAME).delete() || !new File(Database.DATA_FILE_NAME).delete()) {
            fail();
        }
    }

    @Test
    public void testPutAndGet() throws IOException {
        String key = "user123";
        String value = "{\"name\":\"John\",\"age\":30}";

        db.put(key, value);
        db.put("20", "3");

        String retrievedValue = db.get(key);
        assertEquals(value, retrievedValue);
        assertEquals(db.get("20"), "3");
    }

    @Test
    public void testOverwriteData() throws IOException {
        String key = "user123";
        String initialValue = "{\"name\":\"John\",\"age\":30}";
        String updatedValue = "{\"name\":\"Jane\",\"age\":25}";

        db.put(key, initialValue);
        db.put(key, updatedValue);

        String retrievedValue = db.get(key);
        assertEquals(updatedValue, retrievedValue);
    }

    @Test
    public void testDeleteExistingKey() throws IOException {
        String key = "user123";
        String value = "{\"name\":\"John\",\"age\":30}";

        db.put(key, value);
        assertTrue(db.delete(key));
        assertThrows(IOException.class, () -> db.get(key));
    }

    @Test
    public void testDeleteNonExistingKey() throws IOException {
        assertFalse(db.delete("nonExistentKey"));
    }

    @Test
    public void testGetNonExistentData() {
        String key = "nonExistentKey";
        // Here, we're asserting that an exception is thrown.
        // You could also modify the database class to return null and then assert that.
        assertThrows(IOException.class, () -> db.get(key));
    }

    @Test
    public void testConcurrentWrites() throws InterruptedException {
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (int i = 0; i < numThreads; i++) {
            int finalI = i;
            executor.submit(() -> {
                try {
                    db.put("user" + finalI, "{\"name\":\"User" + finalI + "\",\"age\":" + finalI + "}");
                } catch (IOException e) {
                    e.printStackTrace();
                    fail("Failed due to IOException during concurrent writes.");
                }
            });
        }

        executor.shutdown();
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            fail("Threads did not finish in time.");
        }

        // Check data integrity
        for (int i = 0; i < numThreads; i++) {
            try {
                String retrievedValue = db.get("user" + i);
                assertEquals("{\"name\":\"User" + i + "\",\"age\":" + i + "}", retrievedValue);
            } catch (IOException e) {
                e.printStackTrace();
                fail("Failed due to IOException during data integrity check.");
            }
        }
    }

    @Test
    public void testConcurrentReads() throws InterruptedException, IOException {
        String key = "sharedUser";
        String value = "{\"name\":\"Shared User\",\"age\":100}";
        db.put(key, value);

        int numThreads = 500;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    String retrievedValue = db.get(key);
                    assertEquals(value, retrievedValue);
                } catch (IOException e) {
                    e.printStackTrace();
                    fail("Failed due to IOException during concurrent reads.");
                }
            });
        }

        executor.shutdown();
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            fail("Threads did not finish in time.");
        }
    }

    @Test
    public void testCompactWithNoGaps() throws IOException {
        String key1 = "user123";
        String value1 = "{\"name\":\"John\",\"age\":30}";
        db.put(key1, value1);

        String key2 = "user456";
        String value2 = "{\"name\":\"Jane\",\"age\":25}";
        db.put(key2, value2);

        db.compact();

        assertEquals(value1, db.get(key1));
        assertEquals(value2, db.get(key2));
    }

    @Test
    public void testCompactAfterDelete() throws IOException {
        // Assuming there's a delete method in your database
        String key1 = "user123";
        String value1 = "{\"name\":\"John\",\"age\":30}";
        db.put(key1, value1);

        String key2 = "user456";
        String value2 = "{\"name\":\"Jane\",\"age\":25}";
        db.put(key2, value2);

        // Delete a key-value pair to create a gap
        db.delete(key1);

        db.compact();

        // Expect an exception or null value when trying to fetch a deleted key
        assertThrows(IOException.class, () -> db.get(key1));
        assertEquals(value2, db.get(key2));
    }

    @Test
    public void testCompactAfterUpdate() throws IOException {
        String key = "user123";
        String initialValue = "{\"name\":\"John\",\"age\":30}";
        String updatedValue = "{\"name\":\"Jane\",\"age\":25}";

        db.put(key, initialValue);
        db.put(key, updatedValue);

        db.compact();

        assertEquals(updatedValue, db.get(key));
    }
}
