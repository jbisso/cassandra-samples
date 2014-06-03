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
import com.datastax.driver.core.Session;
import java.util.logging.Logger;

/**
 *
 * @author ian
 * @param <T>
 */
public abstract class AbstractFacade<T> {
    private final Class<T> entityClass;
    // C* connection
    private Cluster cluster;
    private Session session;
    
    private static final String CLUSTER_ADDRESS = "localhost";
    private static final String KEYSPACE = "addressbook";
    private static final String TABLE = "contact";
    
    // logger
    private static final Logger LOG = Logger.getLogger(AbstractFacade.class.getName());
    
    

    public AbstractFacade(Class<T> entityClass) {
        this.entityClass = entityClass;
    }

}
