package com.tsystems.dqm.security.util;

/**
 * Placeholder exception for when a required security context is not available.
 */
public class SecurityNotPresentException extends Exception {
    public SecurityNotPresentException(String message) {
        super(message);
    }
}