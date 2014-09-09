package com.example.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;

/*
 * Queries are set at the driver level with two fields: consistency_level and serial_consistency_level
 * 
 * Reads
 * 
 * SERIAL and LOCAL_SERIAL can be used as the (normal) consistency level for reads, 
 * i.e., for reads, you can set consistency_level to SERIAL or LOCAL_SERIAL (or any value)
 * that's what forces an in-progress paxos operation to be committed
 * the serial consistency level (set at the driver level) is ignored for reads
 * 
 * Writes
 * 
 * For writes, SERIAL and LOCAL serial can only be used for the serial consistency level (set at the driver level)
 * and anything except for SERIAL and LOCAL_SERIAL can be used for consistency_level
 * so I think I had it right for writes, but not reads
 */

public class ConsistencyLevelClient extends SimpleClient {
    
    public ConsistencyLevelClient() {
    }
    
    @Override
    public void querySchema() {
        Statement statement = new SimpleStatement("SELECT * FROM simplex.playlists " +
            "WHERE id = 83decbed-14ac-4417-a527-c7c3c1473386;");
        ResultSet results = getSession().execute(statement);
        System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
                "-------------------------------+-----------------------+--------------------"));
        for (Row row : results) {
            System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"),
                    row.getString("album"), row.getString("artist")));
        }
        System.out.println();
    }
    
    @Override
    public void updateSchema() {
        Statement statement = new SimpleStatement(
                "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
                "VALUES (" +
                    "83decbed-14ac-4417-a527-c7c3c1473386," +
                    "c86d796a-f912-440a-a4ab-28fa091f26d3," +
                    "'Der Kommissar'," +
                    "'Einzelhaft'," +
                    "'Falco'" +
                ") IF NOT EXISTS;");
        try {
            statement.setConsistencyLevel(ConsistencyLevel.ALL);
            System.out.println("Consistency level: " + statement.getConsistencyLevel());
            statement.setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
            //statement.setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
            System.out.println("Serial consistency level: " + statement.getSerialConsistencyLevel() + "\n");
            getSession().execute(statement);
        } catch (UnavailableException ue) {
            System.out.println("Cannot perform INSERT, because: " + ue.getMessage());
        } catch (WriteTimeoutException wte) {
            System.out.println("Cannot perform INSERT, because: " + wte.getMessage());
            System.out.println("    Timeout, write type = " + wte.getWriteType());
        }
    }

    public static void main(String[] args) {
        ConsistencyLevelClient client = new ConsistencyLevelClient();
        client.connect("127.0.0.1");
        client.createSchema();
        client.loadData();
        client.pause("Take down a node.");
        client.updateSchema();
        client.querySchema();
        client.pause();
        client.dropSchema("simplex");
        client.close();
    }

}
