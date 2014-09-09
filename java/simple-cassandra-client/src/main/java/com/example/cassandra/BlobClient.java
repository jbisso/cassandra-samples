package com.example.cassandra;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.UnsupportedAudioFileException;

import com.datastax.driver.core.PreparedStatement;

public class BlobClient extends SimpleClient {
    public BlobClient() {
    }
    
    public ByteBuffer readFile(File inputFile) {
        ByteBuffer result = null;
        Date startTimestamp = new Date();
        try {
            AudioInputStream audioInputStream = AudioSystem.getAudioInputStream(inputFile);
            int bytesPerFrame = audioInputStream.getFormat().getFrameSize();
            if (bytesPerFrame == AudioSystem.NOT_SPECIFIED) {
                // some audio formats may have unspecified frame size
                // in that case we may read any amount of bytes
                bytesPerFrame = 1;
            } 
            // Set an arbitrary buffer size of 1024 frames.
            int numBytes = 1024 * bytesPerFrame; 
            byte[] audioBytes = new byte[numBytes];
            byte[] fileAsBytes = new byte[numBytes];
            int numBytesRead = 0;
            int numFramesRead = 0;
            int totalFramesRead = 0;
            // Try to read numBytes bytes from the file.
            while ((numBytesRead = audioInputStream.read(audioBytes)) != -1) {
                // Calculate the number of frames actually read.
                numFramesRead = numBytesRead / bytesPerFrame;
                totalFramesRead += numFramesRead;
                // Add it to the fileAsBytes array after enlargening it.
                byte[] enlargedFileAsBytes = new byte[fileAsBytes.length + numBytes];
                System.arraycopy(fileAsBytes, 0, enlargedFileAsBytes, 0, fileAsBytes.length);
                System.arraycopy(audioBytes, 0, enlargedFileAsBytes, fileAsBytes.length, audioBytes.length);
                fileAsBytes = enlargedFileAsBytes;
            }
            System.out.println("Total frames read: " + totalFramesRead);
            System.out.println("Bytes per frame: " + bytesPerFrame);
            System.out.println("How many bytes in the file: " + fileAsBytes.length);
            result = ByteBuffer.wrap(fileAsBytes);
        } catch (UnsupportedAudioFileException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Date endTimestamp = new Date();
        System.err.println("Time elapsed: " + (endTimestamp.getTime() - startTimestamp.getTime()) / 1000);
        return result;
    }

    public void insertSong(ByteBuffer songBuffer) {
        PreparedStatement preparedInsertStatement = getSession().prepare(
                "INSERT INTO simplex.songs (id, title, album, artist, data) " +
                "VALUES (?, ?, ?, ?, ?); ");
        getSession().execute(preparedInsertStatement.bind(
                UUID.randomUUID(), "Louis Collins", "[Unknown]", "Mississippi John Hurt", songBuffer));
    }

    public static void main(String[] args) {
        File inputFile = new File(System.getProperty("user.dir") + "/src/main/resources/LouisCollins.aiff");
        BlobClient client = new BlobClient();
        client.connect("127.0.0.1");
        client.createSchema();
        client.loadData();
        client.insertSong(client.readFile(inputFile));
        client.pause();
        client.dropSchema("simplex");
        client.close();
    }
}
