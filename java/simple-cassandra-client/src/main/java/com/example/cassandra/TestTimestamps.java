package com.example.cassandra;

import java.util.Date;

public class TestTimestamps {

    public static void main(String[] args) {
        System.out.println(new Date());
        System.out.println(new Date(Long.MIN_VALUE));
        System.out.println(Long.MIN_VALUE);
        System.out.println(new Date(0L));
        System.out.println(0);
        System.out.println(new Date(Long.MAX_VALUE));
        System.out.println(Long.MAX_VALUE);
    }

}
