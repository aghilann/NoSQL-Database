package main;

import java.io.IOException;
import java.io.RandomAccessFile;

public class FileManager {
    private RandomAccessFile file;

    public FileManager(String filename, String mode) throws IOException {
        this.file = new RandomAccessFile(filename, mode);
    }

    public long getFileLength() throws IOException {
        return file.length();
    }

    public long getFilePointer() throws IOException {
        return file.getFilePointer();
    }

    public void seek(long position) throws IOException {
        file.seek(position);
    }

    public void writeLong(long value) throws IOException {
        file.writeLong(value);
    }

    public void writePointerAt(long pointer, long value) throws IOException {
        file.seek(pointer);
        file.writeLong(value);
    }

    public long readPointerAt(long position) throws IOException {
        file.seek(position);
        return file.readLong();
    }

    public int readIntAt(long position) throws IOException {
        file.seek(position);
        return file.readInt();
    }

    public long readLong() throws IOException {
        return file.readLong();
    }

    public void writeInt(int value) throws IOException {
        file.writeInt(value);
    }

    public int readInt() throws IOException {
        return file.readInt();
    }

    public void write(byte[] data) throws IOException {
        file.write(data);
    }

    public void read(byte[] buffer) throws IOException {
        file.read(buffer);
    }

    public void close() throws IOException {
        file.close();
    }

    public void setFile(RandomAccessFile file) {
        this.file = file;
    }
}
