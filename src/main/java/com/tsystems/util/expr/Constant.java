package com.tsystems.util.expr;

/**
 * Placeholder for a generic constant wrapper used in the expression framework.
 */
public class Constant {
    private final Object value;

    public Constant(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }
}