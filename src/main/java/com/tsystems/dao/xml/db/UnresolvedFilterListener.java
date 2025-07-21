package com.tsystems.dao.xml.db;

/**
 * Placeholder for a listener interface that a class can implement
 * to programmatically handle unresolved query filters at runtime.
 */
public interface UnresolvedFilterListener {

    /**
     * This method is called by the data access framework when a filter
     * needs to be resolved.
     * @param e The event object containing details about the filter.
     */
    void unresolvedFilter(UnresolvedFilterEvent e);
}