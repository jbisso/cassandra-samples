package com.example.cassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.UDTMapper;
import com.google.common.collect.ImmutableSet;

public class TwoOneFeatures extends SimpleClient {
    private static String[][] FAKE_USER_DATA = {
        { "756716f7-2e54-4715-9f00-91dcbea6cf50", "John Doe" },
        { "f6071e72-48ec-4fcb-bf3e-379c8a696488", "Jane Quux" },
        //{ "", "", "", "" },
    };
    private static String[][] FAKE_ADDRESS_DATA = {
        { "123 Arnold Drive", "Sonoma", "95476", "707-555-1234" },
        { "31 Puerto Loma Street", "Petaluma", "94952", "800-555-9876" },
        { "4980 Cologne Way", "Sarasota", "34231", "941-555-6547" },
        //{ "", "", "", "" },
    };
    
    private static String INSERT_USER = "INSERT INTO complex.users (id, name, addresses) " +
        "VALUES (:id, :name, :addresses);";
    private PreparedStatement insertUserPreparedStatement;

    public TwoOneFeatures() {
    }
        
    public void prepareStatements() {
        insertUserPreparedStatement = getSession().prepare(INSERT_USER);
    }
    
    public void createSchema() {
        System.out.println("Creating complex schema.");
        getSession().execute(
            "CREATE KEYSPACE complex WITH replication " + 
                "= {'class':'SimpleStrategy', 'replication_factor':3};");
        getSession().execute(
            "CREATE TYPE phone (" +
                    "alias text," +
                    "number text)"
            );
        getSession().execute(
            "CREATE TYPE complex.address (" +
                    "street text," +
                    "city text," +
                    "zip_code int," +
                    "phones list<phone>);"
            );
        // create a table that uses it
        getSession().execute(
            "CREATE TABLE complex.users (" +
                "id uuid PRIMARY KEY," +
                "name text," +
                "addresses map<text, address>);"
            );
        getSession().execute("CREATE TABLE complex.accounts (" +
                "email text PRIMARY KEY," +
                "name text);"
            );
    }
    
    private Address getAddress(int number) {
        Address result = new Address();
        result.setStreet(FAKE_ADDRESS_DATA[number][0]);
        result.setCity(FAKE_ADDRESS_DATA[number][1]);
        result.setZipCode(Integer.parseInt(FAKE_ADDRESS_DATA[number][2]));
        List<String> phones = new ArrayList<String>();
        phones.add(FAKE_ADDRESS_DATA[number][3]);
        result.setPhones(phones);
        return result;
    }
    
    private User getUser(int number) {
        User result = new User();
        result.setId(UUID.fromString(FAKE_USER_DATA[number][0]));
        result.setName(FAKE_USER_DATA[number][1]);
        Map<String, Address> addresses = new HashMap<String, Address>();
        Address address = getAddress(number);
        addresses.put("Home", address);
        result.setAddresses(addresses);        
        return result;
    }
    
    public void loadData() {
        for (int i = 0; i < 2; ++i ) {
            User user = getUser(i);
            BoundStatement boundStatement = insertUserPreparedStatement.bind(
                    user.getId(),
                    user.getName()
                );
            getSession().execute(boundStatement);
        }
    }
    
    public void loadDataUsingRaw() {
        PreparedStatement insertUserPreparedStatement
            = getSession().prepare("INSERT INTO users (id, addresses) VALUES (?, ?);");
        PreparedStatement selectUserPreparedStatement
            = getSession().prepare("SELECT * FROM users WHERE id = ?;");

         UserType addressUDT = getSession().getCluster()
            .getMetadata().getKeyspace("complex").getUserType("address");
         UserType phoneUDT = getSession().getCluster()
            .getMetadata().getKeyspace("complex").getUserType("phone");
    
         UDTValue phone1 = phoneUDT.newValue()
            .setString("alias", "home")
            .setString("number", "1");
         UDTValue phone2 = phoneUDT.newValue()
            .setString("alias", "work")
            .setString("number", "0698265251");
    
         UDTValue addresses = addressUDT.newValue()
            .setString("street", "1600 Pennsylvania Ave NW")
            .setInt("zip_code", 20500)
            .setSet("phones", ImmutableSet.of(phone1, phone2));

         UUID userId = UUID.fromString("fbdf82ed-0063-4796-9c7c-a3d4f47b4b25");
         getSession().execute(insertUserPreparedStatement.bind(userId, addresses));
         
         Row row = getSession().execute(selectUserPreparedStatement.bind(userId)).one();
         for ( Address address : row.getMap("addresses", String.class, Address.class).values() ) {
             System.out.println("Zip: " + address.getZipCode());
         }
    }
    
    public void loadDataUsingMapper() {
        System.out.println("Loading data into complex schema.");
        Mapper<User> mapper = new MappingManager(getSession()).mapper(User.class);
        User user = getUser(0);
        mapper.save(user);
    }
    
    public void testMappings() {
        Mapper<Account> mapper = new MappingManager(getSession()).mapper(Account.class);
        Account account = new Account("John Doe", "jd@example.com");
        mapper.save(account);
        Account whose = mapper.get("jd@example.com");
        System.out.println("Account name: " + whose.getName());
        mapper.delete(account);
    }
    
    public void querySchema() {
        System.out.println("Querying data.");
        UDTMapper<Address> mapper = new MappingManager(getSession()).udtMapper(Address.class);
        ResultSet results = getSession().execute("SELECT * FROM complex.users " +
            "WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");
        for (Row row : results) {
            System.out.println(row.getString("name"));
            Map<String, UDTValue> addresses = row.getMap("addresses", String.class, UDTValue.class);
            for (String key : addresses.keySet()) {
                Address address = mapper.map(addresses.get(key));
                System.out.println(key + " address: " + address);
            }
        }
    }

    public static void main(String[] args) {
        TwoOneFeatures client = new TwoOneFeatures();
        client.connect("ec2-54-176-125-19.us-west-1.compute.amazonaws.com");
        client.createSchema();
        client.prepareStatements();
        client.loadData();
        client.querySchema();
        //client.dropSchema("complex");
        client.close();
    }
}
