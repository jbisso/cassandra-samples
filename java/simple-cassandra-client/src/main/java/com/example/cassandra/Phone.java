package com.example.cassandra;

public class Phone {
    private String alias;
    private String number;

    public Phone() {
    }
    
    public Phone(String alias, String number) {
        this.alias = alias;
        this.number = number;
    }
    
    public String getAlias() {
        return alias;
    }
    public void setAlias(String alias) {
        this.alias = alias;
    }
    public String getNumber() {
        return number;
    }
    public void setNumber(String number) {
        this.number = number;
    }
}
