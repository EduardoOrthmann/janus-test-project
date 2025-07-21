package com.tsystems.utils.exceptions;

import java.sql.SQLException;

/**
 * Placeholder for a utility class designed to inspect and format a chain
 * of SQLExceptions to produce a comprehensive error message.
 */
public class ChainedSQLExceptionWalker {

    /**
     * Walks through a chain of SQLExceptions and concatenates their messages.
     *
     * @param e The initial SQLException.
     * @return A string containing all messages from the exception chain.
     */
    public static String getAllMessages(SQLException e) {
        if (e == null) {
            return "No SQLException provided.";
        }
        StringBuilder messages = new StringBuilder();
        messages.append("Chained Exception: ").append(e.getMessage());

        // In a real implementation, this would loop through e.getNextException()
        SQLException nextEx = e.getNextException();
        int count = 1;
        while (nextEx != null) {
            messages.append("\n  -> Caused by (").append(count++).append("): ").append(nextEx.getMessage());
            nextEx = nextEx.getNextException();
        }

        return messages.toString();
    }
}