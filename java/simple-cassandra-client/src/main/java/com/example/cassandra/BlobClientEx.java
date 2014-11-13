package com.example.cassandra;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.utils.Bytes;

public class BlobClientEx extends SimpleClient {
    private PreparedStatement preparedInsertStatement;
    private PreparedStatement preparedRerieveStatement;
    
    public BlobClientEx() { }

    public void prepareStatements() {
        preparedInsertStatement = getSession().prepare(
                "INSERT INTO simplex.songs (id, title, album, artist, data) " +
                "VALUES (?, ?, ?, ?, ?); ");
        preparedRerieveStatement = getSession().prepare("SELECT * FROM simplex.songs WHERE id = ?;");
    }

    public UUID insertSong() {
        UUID result = UUID.randomUUID();
        byte[] songBytes = new byte[50];
        for (byte index = 0; index < 50; ++index) {
            songBytes[index] = index;
        }
        ByteBuffer blobBuffer = ByteBuffer.allocate(50);
        blobBuffer.put(songBytes);
        blobBuffer.clear();
        getSession().execute(preparedInsertStatement.bind(
                result, "Louis Collins", "[Unknown]", "Mississippi John Hurt", blobBuffer));
        return result;
    }
    
    public ByteBuffer retrieveSong(UUID id) {
        ResultSet results = getSession().execute(preparedRerieveStatement.bind(id));
        ByteBuffer data = results.one().getBytes("data");
        byte[] bytes = Bytes.getArray(data);
        System.out.print("[");
        for (byte index = 0; index < 50; ++index) {
            System.out.print(bytes[index] + ", ");
        }
        System.out.println("]");
        return data;
    }
    
    /*
    public static byte[] getArray(ByteBuffer bytes) {
        int length = bytes.remaining();

        if (bytes.hasArray()) {
            int boff = bytes.arrayOffset() + bytes.position();
            if (boff == 0 && length == bytes.array().length)
                return bytes.array();
            else
                return Arrays.copyOfRange(bytes.array(), boff, boff + length);
        }
        // else, DirectByteBuffer.get() is the fastest route
        byte[] array = new byte[length];
        bytes.duplicate().get(array);
        return array;
    }
     */
   
    public static void main(String[] args) {
        BlobClientEx client = new BlobClientEx();
        client.connect("127.0.0.1");
        client.createSchema();
        client.prepareStatements();
        client.loadData();
        
        UUID id = client.insertSong();
        ByteBuffer byteBuffer = client.retrieveSong(id);
        
        client.pause();
        client.dropSchema("simplex");
        client.close();
    }
}
