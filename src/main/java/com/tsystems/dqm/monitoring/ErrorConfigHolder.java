package com.tsystems.dqm.monitoring;

/**
 * A placeholder class that acts as a data holder for error configuration settings.
 * It is used to pass around configuration data retrieved from the database.
 */
public class ErrorConfigHolder {

    private Integer devAbove;
    private Integer devBelow;
    private Integer targetFrom;
    private Integer targetTo;

    public void setDevAbove(Integer devAbove) {
        this.devAbove = devAbove;
    }

    public Integer getDevAbove() {
        return devAbove;
    }

    public void setDevBelow(Integer devBelow) {
        this.devBelow = devBelow;
    }

    public Integer getDevBelow() {
        return devBelow;
    }

    public void setTargetFrom(Integer targetFrom) {
        this.targetFrom = targetFrom;
    }

    public Integer getTargetFrom() {
        return targetFrom;
    }

    public void setTargetTo(Integer targetTo) {
        this.targetTo = targetTo;
    }

    public Integer getTargetTo() {
        return targetTo;
    }
}