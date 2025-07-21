package com.tsystems.util.expr;

/**
 * Placeholder for a utility class that wraps a value to indicate it should
 * be treated as an identity or literal value in a SQL query, rather than a
 * standard parameter to be escaped or quoted.
 */
public class Identity {

    private final String value;

    public Identity(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}