package main;

import main.Database;

import java.io.IOException;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static void main(String[] args) {
        try {
            Database db = new Database();

            // Put some data
            db.put("user123", "{\"name\":\"John\",\"age\":30}");
            db.put("user456", "{\"name\":\"Jane\",\"age\":25}");

            // Retrieve and print the data
            String userData1 = db.get("user123");
            System.out.println("Data for user123: " + userData1);

            String userData2 = db.get("user456");
            System.out.println("Data for user456: " + userData2);

            db.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
