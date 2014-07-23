package com.example.cassandra;

import java.util.List;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import com.google.common.base.Objects;

@UDT (keyspace = "complex", name = "address")
public class Address {
    private String street;
    
    private String city;
    
    @Field (name = "zip_code")
    private int zipCode;
    
    private List<String> phones;

    public Address() {
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public int getZipCode() {
        return zipCode;
    }

    public void setZipCode(int zipCode) {
        this.zipCode = zipCode;
    }

    public List<String> getPhones() {
        return phones;
    }

    public void setPhones(List<String> phones) {
        this.phones = phones;
    }
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof Address) {
            Address that = (Address) other;
            return Objects.equal(this.street, that.street) &&
                   Objects.equal(this.city, that.city) &&
                   Objects.equal(this.zipCode, that.zipCode) &&
                   Objects.equal(this.phones, that.phones);
        }
        return false;
    }
    
    @Override
    public String toString() {
        return "{ " + street + ", " + city + ", " + zipCode + " phones: " + phones + " }";
    }
}
