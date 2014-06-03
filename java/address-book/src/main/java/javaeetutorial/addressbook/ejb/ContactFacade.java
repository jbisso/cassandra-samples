/**
 * Copyright (c) 2014 Oracle and/or its affiliates. All rights reserved.
 *
 * You may not modify, use, reproduce, or distribute this software except in
 * compliance with  the terms of the License at:
 * http://java.net/projects/javaeetutorial/pages/BerkeleyLicense
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package javaeetutorial.addressbook.ejb;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javaeetutorial.addressbook.entity.Contact;
import javax.annotation.PostConstruct;
import javax.ejb.Stateless;

/**
 *
 * @author ian
 */
@Stateless
public class ContactFacade extends AbstractFacade<Contact> {
    // C* connection
    private Cluster cluster;
    private Session session;
    
    private static final String CLUSTER_ADDRESS = "localhost";
    private static final String KEYSPACE = "addressbook";
    private static final String TABLE = "contact";
    private static final Logger LOG = Logger.getLogger(ContactFacade.class.getName());

    public ContactFacade() {
        super(Contact.class);
    }
    @PostConstruct
    private void init() {
        cluster = Cluster.builder()
                .addContactPoint(CLUSTER_ADDRESS)
                .build();
        Metadata metadata = cluster.getMetadata();
        LOG.log(Level.INFO, "Connected to cluster {0}", metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            LOG.log(Level.INFO, "Data center: {0}; IP address: {1}", new Object[]{host.getDatacenter(), host.getAddress()});
        }
        session = cluster.connect();
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE
                + " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};");
        session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE +"." + TABLE +" ("
                + "id uuid PRIMARY KEY, "
                + "firstName text, "
                + "lastName text, "
                + "email text, "
                + "mobilePhone text, "
                + "homePhone text, "
                + "birthday timestamp);");
        
    }

    public void create(Contact contact) {
        Insert insert = QueryBuilder.insertInto(KEYSPACE, TABLE)
                .value("id", UUIDs.random())
                .value("firstName", contact.getFirstName())
                .value("lastName", contact.getLastName())
                .value("email", contact.getEmail())
                .value("mobilePhone", contact.getMobilePhone())
                .value("homePhone", contact.getHomePhone())
                .value("birthday", contact.getBirthday());
        session.execute(insert);
    }

    public void edit(Contact contact) {
        Statement update = QueryBuilder.update(KEYSPACE, TABLE)
                .with(QueryBuilder.set("firstName", contact.getFirstName()))
                .and(QueryBuilder.set("lastName", contact.getLastName()))
                .and(QueryBuilder.set("email", contact.getEmail()))
                .and(QueryBuilder.set("mobilePhone", contact.getMobilePhone()))
                .and(QueryBuilder.set("homePhone", contact.getHomePhone()))
                .and(QueryBuilder.set("birthday", contact.getBirthday()))
                .where(QueryBuilder.eq("id", contact.getId()));
        session.execute(update);
    }

    public void remove(Contact contact) {
        Statement delete = QueryBuilder.delete()
                .from(KEYSPACE, TABLE)
                .where(QueryBuilder.eq("id", contact.getId()));
        session.execute(delete);
    }

    public Contact find(Object id) {
        Statement select = QueryBuilder.select()
                .distinct()
                .from(KEYSPACE, TABLE)
                .where(QueryBuilder.eq("uuid", id));
        Row result = session.execute(select).one();
        Contact contact = new Contact();
        contact.setId(result.getUUID("id"));
        contact.setFirstName(result.getString("firstName"));
        contact.setLastName(result.getString("lastName"));
        contact.setEmail(result.getString("email"));
        contact.setMobilePhone(result.getString("mobilePHone"));
        contact.setHomePhone(result.getString("homePhone"));
        contact.setBirthday(result.getDate("birthday"));
        return contact;
    }

    public List<Contact> findAll() {
        List<Contact> contacts = new ArrayList<>();
        Select query = QueryBuilder.select()
                .all()
                .from(KEYSPACE, TABLE);
        List<Row> results = session.execute(query).all();
        for (Row row : results) {
            contacts.add(new Contact(row.getUUID("id"), 
                    row.getString("firstName"),
                    row.getString("lastName"),
                    row.getString("email"),
                    row.getString("mobilePhone"),
                    row.getString("homePhone"),
                    row.getDate("birthday")));
        }
        return contacts;
    }


    public int count() {
        // return the total number of entries
        Select query = QueryBuilder.select()
                .countAll()
                .from(KEYSPACE, TABLE);
        return session.execute(query).one().getInt("count");
    }


}
