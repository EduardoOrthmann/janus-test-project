package com.tsystems.dqm.monitoring;

/**
 * Represents the type of object being monitored for errors, such as
 * a consignment or a transport.
 */
public enum MonitoringObjectType {
    CONSIGNMENT("C"),
    TRANSPORT("T");

    private final String type;

    MonitoringObjectType(String type) {
        this.type = type;
    }

    /**
     * Returns the short string representation of the object type,
     * likely used for storing in the database.
     * @return The single-character type identifier.
     */
    public String getType() {
        return this.type;
    }
}