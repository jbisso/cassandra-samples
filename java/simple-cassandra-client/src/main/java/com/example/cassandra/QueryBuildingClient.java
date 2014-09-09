package com.example.cassandra;

import java.util.UUID;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class QueryBuildingClient extends SimpleClient {
    QueryBuildingClient() {
    }
    
    public void useSelect() {
        System.out.println("Selecting from simplex.songs table.");
        Statement statement = QueryBuilder.select()
                .all()
                .from("simplex", "songs")
                .where(eq("id", UUID.fromString("f6071e72-48ec-4fcb-bf3e-379c8a696488")));
        ResultSet results = getSession().execute(statement);
        for ( Row row : results ) {
            System.out.println("Song: " + row.getString("artist"));
        }
    }
    
    public void useInsert() {
        System.out.println("Inserting into simplex.songs table.");
        Statement statement = QueryBuilder.insertInto("simplex", "songs")
                .value("id", UUID.fromString("ad760c30-1d88-11e4-8c21-0800200c9a66"))
                .value("artist", "Harry Nilsson")
                .value("title", "Coconut")
                .value("album", "Nilsson Schmilsson");
        getSession().execute(statement);
    }
    
    public void useUpdate() {
        System.out.println("Updating simplex.songs table.");
        Statement statement = QueryBuilder.update("simplex", "songs")
            .with(set("artist", "Vasili Ostertag"))
            .where(eq("id", UUID.fromString("f6071e72-48ec-4fcb-bf3e-379c8a696488")));
        getSession().execute(statement);
    }

    public void useDelete() {
        System.out.println("Deleting from simplex.songs table.");
        Statement statement = QueryBuilder.delete()
                .from("simplex", "songs")
                .where(eq("id", UUID.fromString("f6071e72-48ec-4fcb-bf3e-379c8a696488")));
        getSession().execute(statement);
    }
    
    public void anotherone() {
        Statement statement = QueryBuilder.select()
                .all()
                .from("title", "playlists");
        ResultSet results = getSession().execute(statement);
        for ( Row row : results ) {
            System.out.println("Playlist: " + row.getString("title"));
        }
    }

    public static void main(String[] args) {
        QueryBuildingClient client = new QueryBuildingClient();
        client.connect("127.0.0.1");
        client.createSchema();
        client.loadData();
        client.useInsert();
        client.useUpdate();
        client.useSelect();
        client.pause();
        client.dropSchema("simplex");
        client.close();
    }

}
