package com.example.cassandra;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.AlreadyExistsException;

public class CqlCollectionsExample extends SimpleClient {

    // Schema creation statements
    private final String CREATE_SCHEMA_STATEMENT =
            "CREATE KEYSPACE ds_social " +
            "WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};";
    private final String CREATE_USERS_TABLE = "CREATE TABLE ds_social.users (" +
            "user text PRIMARY KEY, " +
            "tasks list<text>, " +
            "tags set<text>, " +
            "posts map<timestamp, text>" +
            ");";

    // Insertion statements
    private final String INSERT_USERS_DATA_SIMPLE = "INSERT INTO ds_social.users (user, tasks, tags, posts) VALUES ";
    private final String INSERT_USERS_DATA_PREPARED = INSERT_USERS_DATA_SIMPLE + "(?, ?, ?, ?);";

    // Selection statement
    private final String SELECT_ALL_USERS = "SELECT * FROM ds_social.users;";

    public CqlCollectionsExample() {
    }

    @Override
    public void createSchema() {
        try {
            getSession().execute(CREATE_SCHEMA_STATEMENT);
        } catch (AlreadyExistsException aee) {
            System.err.println("Keyspace already exists, deleting it.");
            dropSchema("ds_social");
            getSession().execute(CREATE_SCHEMA_STATEMENT);
        }
        getSession().execute(CREATE_USERS_TABLE);
        System.out.println("ds_social keyspace and schema created.");
    }

    @Override
    public void loadData() {
        // insert data using a simple statement
        getSession().execute(INSERT_USERS_DATA_SIMPLE +
                "('frodo', ['Eating second breakfast.', 'Questing.'], {'mithril', 'ring'}, " +
                "{'2013-10-11 10:45:00' : 'Fair to middling.', '2013-11-26 13:11:23' : 'High in the 80s.'});");
        System.out.println("Data loaded into users table with simple statement.");

        // insert data with prepared statement bound to Java types
        PreparedStatement statement = getSession().prepare(INSERT_USERS_DATA_PREPARED);
        BoundStatement boundStatement = new BoundStatement(statement);

        String user = "golem";

        List<String> tasks = new ArrayList<String>();
        tasks.add("Fishing.");
        tasks.add("Riddling.");

        Set<String> tags = new HashSet<String>();
        tags.add("ring");

        Map<Date, String> posts = new HashMap<Date, String>();

        getSession().execute(boundStatement.bind(user, tasks, tags, posts));
        System.out.println("Data loaded into users table with prepared statement.");
    }

    @Override
    public void querySchema() {
        ResultSet results = getSession().execute(SELECT_ALL_USERS);

        System.out.println(String.format("%-30s\t%-30s\t%-30s\t%-30s\n%s", "user", "tags", "tasks", "posts",
                "-------------------------------+-----------------------+--------------------"));
        for (Row row : results) {
            System.out.println(String.format("%-30s\t%-30s\t%-30s\t%-30s", row.getString("user"),
                    row.getList("tasks", String.class), row.getSet("tags", String.class),
                    row.getMap("posts", Date.class, String.class)));
        }
        System.out.println();
    }

    public static void main(String[] args) {
        CqlCollectionsExample client = new CqlCollectionsExample();
        client.connect("127.0.0.1");
        client.createSchema();
        client.loadData();
        client.querySchema();
        client.dropSchema("ds_social");
        client.close();
    }

}
