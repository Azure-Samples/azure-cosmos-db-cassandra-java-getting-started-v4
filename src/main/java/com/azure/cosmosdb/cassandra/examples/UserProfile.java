package com.azure.cosmosdb.cassandra.examples;

import com.azure.cosmosdb.cassandra.repository.UserRepository;
import com.azure.cosmosdb.cassandra.util.CassandraUtils;
import com.datastax.oss.driver.api.core.CqlSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example class which will demonstrate following operations on Cassandra
 * Database on CosmosDB - Create Keyspace - Create Table - Insert Rows - Select
 * all data from a table - Select a row from a table
 */
public class UserProfile {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserProfile.class);

    public static void main(String[] s) throws Exception {

        CassandraUtils utils = new CassandraUtils();
        CqlSession cassandraSession = utils.getSession();

        final String keyspace = "uprofile";
        final String table = "user";

        try {
            UserRepository repository = new UserRepository(cassandraSession, keyspace, table);

            //Drop keyspace in cassandra database if exists
            repository.dropKeyspace();      
            Thread.sleep(2000);

            //Create keyspace in cassandra database
            repository.createKeyspace();
            Thread.sleep(2000);
            
            //Create table in cassandra database
            repository.createTable();
            Thread.sleep(2000);

            //Insert rows into user table
            String preparedStatement = repository.prepareInsertStatement();
            repository.insertUser(preparedStatement, 1, "LyubovK", "Bangalore");
            repository.insertUser(preparedStatement, 2, "JiriK", "Mumbai");
            repository.insertUser(preparedStatement, 3, "IvanH", "Belgum");
            repository.insertUser(preparedStatement, 4, "YuliaT", "Gurgaon");
            repository.insertUser(preparedStatement, 5, "IvanaV", "Dubai");

            LOGGER.info("Select all users");
            repository.selectAllUsers();

            LOGGER.info("Select a user by id (3)");
            repository.selectUser(3);
        }
        finally {
            utils.close();
            LOGGER.info("Please delete your table after verifying the presence of the data in portal or from CQL");
        }
    }
}
