package com.tsystems.util.expr;

/**
 * Placeholder for a specific type of Constant that represents a SQL NULL value,
 * often including the SQL data type.
 */
public class Null extends Constant {
    private final int sqlType;

    public Null(int sqlType) {
        super(null);
        this.sqlType = sqlType;
    }

    public int getSqlType() {
        return sqlType;
    }
}