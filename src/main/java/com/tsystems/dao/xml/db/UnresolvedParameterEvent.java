package com.tsystems.dao.xml.db;

/**
 * Placeholder for an event object that is created when a query parameter
 * cannot be resolved automatically by the framework.
 */
public class UnresolvedParameterEvent {
    private String name;
    private Object value;

    public UnresolvedParameterEvent() {
        // Default constructor
    }

    public String getName() {
        return this.name;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}