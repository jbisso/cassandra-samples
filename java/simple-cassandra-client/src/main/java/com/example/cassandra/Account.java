
package com.example.cassandra;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.base.Objects;

@Table(keyspace = "complex", name = "accounts")
public class Account {
    @PartitionKey
    private String email;
    private String name;

    public Account() {
    }

    public Account(String name, String email) {
        this.name = name;
        this.email = email;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof Account) {
            Account that = (Account) other;
            return Objects.equal(this.name, that.name) &&
                   Objects.equal(this.email, that.email);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, email);
    }
}
