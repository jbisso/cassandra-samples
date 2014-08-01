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
import com.google.common.collect.ImmutableList;

public class TwoOneFeatures extends SimpleClient {
    private static String[][] FAKE_USER_DATA = {
        { "756716f7-2e54-4715-9f00-91dcbea6cf50", "John Doe" },
        { "f6071e72-48ec-4fcb-bf3e-379c8a696488", "Jane Quux" },
        { "93031620-12ae-11e4-9191-0800200c9a66", "Gary Binary" },
        //{ "", "", "", "" },
    };
    private static String[][] FAKE_ADDRESS_DATA = {
        { "123 Arnold Drive", "Sonoma", "95476", "707-555-1234" },
        { "31 Puerto Loma Street", "Petaluma", "94952", "800-555-9876" },
        { "4980 Cologne Way", "Sarasota", "34231", "941-555-6547" },
        //{ "", "", "", "" },
    };
    
    private static String INSERT_USER = "INSERT INTO complex.users (id, name, addresses) " +
        "VALUES (?, ?, ?);";
    private PreparedStatement insertUserPreparedStatement;
    private PreparedStatement selectUserPreparedStatement;

    public TwoOneFeatures() {
    }
        
    public void prepareStatements() {
        insertUserPreparedStatement = getSession().prepare(INSERT_USER);
        selectUserPreparedStatement = getSession().prepare("SELECT * FROM complex.users WHERE id = ?;");
    }
    
    public void createSchema() {
        System.out.println("Creating complex schema.");
        getSession().execute(
            "CREATE KEYSPACE complex WITH replication " + 
                "= {'class':'SimpleStrategy', 'replication_factor':3};");
        getSession().execute(
            "CREATE TYPE complex.phone (" +
                    "alias text," +
                    "number text);"
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
                "name text," +
                "addr address);"
            );
    }
    
    private Address getAddress(int number) {
        Address result = new Address();
        result.setStreet(FAKE_ADDRESS_DATA[number][0]);
        result.setCity(FAKE_ADDRESS_DATA[number][1]);
        result.setZipCode(Integer.parseInt(FAKE_ADDRESS_DATA[number][2]));
        List<Phone> phones = new ArrayList<Phone>();
        Phone phone = new Phone("home", FAKE_ADDRESS_DATA[number][3]);
        phones.add(phone);
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
        System.out.println("Loading data into complex schema with queries.");
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
        System.out.println("Loading data into complex schema using raw API.");

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
    
         UDTValue address = addressUDT.newValue()
            .setString("street", "1600 Pennsylvania Ave NW")
            .setInt("zip_code", 20500)
            .setList("phones", ImmutableList.of(phone1, phone2));
         Map<String, UDTValue> addresses = new HashMap<String, UDTValue>();
         addresses.put("Work", address);

         UUID userId = UUID.fromString("fbdf82ed-0063-4796-9c7c-a3d4f47b4b25");
         getSession().execute(insertUserPreparedStatement.bind(userId, "G. Binary", addresses));
         
         Row row = getSession().execute(selectUserPreparedStatement.bind(userId)).one();
         for ( UDTValue addr : row.getMap("addresses", String.class, UDTValue.class).values() ) {
             System.out.println("Zip: " + addr.getInt("zip_code"));
         }
         
         // Direct field manipulation
         //ResultSet rs = getSession().execute("SELECT addresses['Work'] FROM complex.users WHERE id = ?", userId);
         //int zip = rs.one().getInt("addresses['Work'].zip_code");
    }
    
    public void loadDataUsingMapper() {
        System.out.println("Loading data into complex schema using mapping API.");
        Mapper<User> mapper = new MappingManager(getSession()).mapper(User.class);
        User user = getUser(2);
        System.out.println("name: " + user.getName());
        mapper.save(user);
    }
    
    public void testMappings() {
        System.out.println("Testing mapper.");
        Mapper<Account> mapper = new MappingManager(getSession()).mapper(Account.class);
        Phone phone = new Phone("home", "707-555-3537");
        List<Phone> phones = new ArrayList<Phone>();
        phones.add(phone);
        Address address = new Address("25800 Arnold Drive", "Sonoma", 95476, phones);
        Account account = new Account("John Doe", "jd@example.com", address);
        mapper.save(account);
        Account whose = mapper.get("jd@example.com");
        System.out.println("Account name: " + whose.getName());
        mapper.delete(account);
    }
    
    public void mapUdtToField() {
        getSession().execute(
                "CREATE TABLE complex.customers (" +
                    "email text PRIMARY KEY," +
                    "phone_number phone);"
                );
        PreparedStatement preparedStatement = getSession()
                .prepare("INSERT INTO complex.customers (email, phone_number) VALUES (?, ?);");
        UserType phoneUDT = getSession().getCluster()
                .getMetadata()
                .getKeyspace("complex")
                .getUserType("phone");
        UDTValue phone = phoneUDT.newValue()
                .setString("alias", "home")
                .setString("number", "707-555-7654");
        getSession().execute(preparedStatement.bind("grex@example.com", phone));
        ResultSet results = getSession()
                .execute("SELECT phone_number.number FROM " + 
                        "complex.customers WHERE email = 'grex@example.com';");
        String number = results.one().getString("phone_number.number");
        System.out.println("Phone number: " + number);
        
        // Update
        preparedStatement = getSession()
                .prepare("UPDATE complex.customers SET phone_number = " +
                        "{ number : ? } WHERE email = 'grex@example.com';");
        getSession().execute(preparedStatement.bind("510-555-1209"));
        results = getSession()
                .execute("SELECT * FROM " + 
                        "complex.customers WHERE email = 'grex@example.com';");
        UDTValue value = results.one().getUDTValue("phone_number");
        System.out.println("Phone number: " + value.getString("number"));
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
        client.connect("127.0.0.1");
        client.createSchema();
        client.prepareStatements();
        client.loadData();
        client.querySchema();
        client.loadDataUsingRaw();
        client.mapUdtToField();
        client.testMappings();
        client.pause();
        client.loadDataUsingMapper();
        client.dropSchema("complex");
        client.close();
    }
}
