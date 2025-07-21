package com.tsystems.dao.xml.db;

import java.util.logging.Logger;

/**
 * Placeholder for an event object that is created when a dynamic filter
 * in a query cannot be resolved. This would be passed to a listener.
 */
public class UnresolvedFilterEvent {

    private static final Logger LOGGER = Logger.getLogger(UnresolvedFilterEvent.class.getName());

    public UnresolvedFilterEvent() {
        // Default constructor
    }

    public static void unresolvedFilter(UnresolvedFilterEvent e) {
        LOGGER.warning("Handling unresolved filter event: " + e.toString());
    }
}