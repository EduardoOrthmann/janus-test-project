package com.tsystems.dqm.monitoring.exception;

import com.tsystems.dao.xml.DAOException;

/**
 * Placeholder for a specific DAO exception that is thrown when a unique
 * column constraint violation occurs during a database operation.
 */
public class DAOColumnUniqueException extends DAOException {

    public DAOColumnUniqueException(String message, Throwable cause) {
        super(message, cause);
    }
}