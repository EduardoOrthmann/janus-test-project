package com.tsystems.dao.xml.db;

/**
 * Placeholder for a listener interface that allows a class to handle
 * unresolved query parameters programmatically.
 */
public interface UnresolvedParameterListener {

    /**
     * This method is called by the data access framework when a parameter
     * needs a value.
     * @param e The event object containing details about the parameter.
     */
    void unresolvedParameter(UnresolvedParameterEvent e);
}