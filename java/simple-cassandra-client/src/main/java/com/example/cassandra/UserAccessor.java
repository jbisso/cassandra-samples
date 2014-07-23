package com.example.cassandra;

import java.util.UUID;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;

@Accessor
public interface UserAccessor {
    @Query("SELECT * FROM complex.users WHERE id=:id")
    User getOne(@Param("userId") UUID id);

    @Query("UPDATE complex.users SET addresses[:name]=:address WHERE id=:id")
    ResultSet addAddress(@Param("id") UUID id, @Param("name") String addressName, @Param("address") Address address);

    @Query("SELECT * FROM complex.users")
    public Result<User> getAll();
}
