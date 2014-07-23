package com.example.cassandra;

import java.util.Map;
import java.util.UUID;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.base.Objects;

@Table(keyspace = "complex", name = "users")
public class User {
    @PartitionKey
    private UUID id;
    
    private String name;
    
    private Map<String, Address> addresses;

    public User() {
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Address> getAddresses() {
        return addresses;
    }

    public void setAddresses(Map<String, Address> addresses) {
        this.addresses = addresses;
    }
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof User) {
            User that = (User) other;
            return Objects.equal(this.id, that.id) &&
                   Objects.equal(this.name, that.name) &&
                   Objects.equal(this.addresses, that.addresses);
        }
        return false;
    }
}

