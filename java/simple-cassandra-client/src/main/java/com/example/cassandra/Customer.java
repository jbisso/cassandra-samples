package com.example.cassandra;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table ( keyspace = "complex", name = "customers")
public class Customer {
    @PartitionKey
    private String email;
    @Column( name = "phone_number" )
    private Phone phoneNumber;
    
    public Customer() {
    }
   
    public Customer(String email, Phone phoneNumber) {
        this.email = email;
        this.phoneNumber = phoneNumber;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public Phone getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(Phone phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

}
