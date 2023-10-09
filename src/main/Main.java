package main;

import main.Database;

import java.io.IOException;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static void main(String[] args) {
        try {
            Database db = new Database(false);
            db.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
