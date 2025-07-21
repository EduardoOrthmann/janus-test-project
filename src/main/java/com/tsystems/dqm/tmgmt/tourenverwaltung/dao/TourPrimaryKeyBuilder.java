package com.tsystems.dqm.tmgmt.tourenverwaltung.dao;

import com.tsystems.dao.xml.DAOException;
import com.tsystems.dqm.tmgmt.tourenverwaltung.interfaces.TourPrimaryKey;
import java.sql.Connection;

/**
 * Placeholder for an interface responsible for building primary keys for Tour entities.
 * This pattern separates key generation logic from the main DAO operations.
 */
public interface TourPrimaryKeyBuilder {

    /**
     * Builds a primary key object from an existing ID.
     * @param id The existing ID.
     * @return A TourPrimaryKey object.
     */
    TourPrimaryKey buildPrimaryKey(long id);

    /**
     * Builds a new primary key, typically by fetching a new value from a database sequence.
     * @param connection The database connection to use for the operation.
     * @return A new TourPrimaryKey object.
     * @throws DAOException if the key cannot be generated.
     */
    TourPrimaryKey buildNewPrimaryKey(Connection connection) throws DAOException;
}