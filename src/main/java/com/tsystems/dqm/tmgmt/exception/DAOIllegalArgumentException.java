package com.tsystems.dqm.tmgmt.exception;

import com.tsystems.dao.xml.DAOException;

/**
 * Placeholder for an exception thrown when a DAO method is called with
 * illegal or inappropriate arguments.
 */
public class DAOIllegalArgumentException extends DAOException {
    public DAOIllegalArgumentException(String message) {
        super(message);
    }
}