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
        db = new Database();
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

        String retrievedValue = db.get(key);
        assertEquals(value, retrievedValue);
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
    public void testCompaction() throws IOException {
        // Perform 100 writes
        for (int i = 0; i < 100; i++) {
            db.put("key" + i, "value" + i);
        }

        // Perform 100 deletions
        for (int i = 0; i < 100; i++) {
            db.delete("key" + i);
        }

        // Now, the data file should be at its initial or minimum size
        File dataFile = new File(Database.DATA_FILE_NAME);
        assertEquals(0, dataFile.length(), "Data file should be empty after deletions");
    }
}
