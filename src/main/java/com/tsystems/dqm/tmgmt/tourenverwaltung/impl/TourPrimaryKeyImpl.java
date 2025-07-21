package com.tsystems.dqm.tmgmt.tourenverwaltung.impl;
import com.tsystems.dqm.tmgmt.tourenverwaltung.interfaces.TourPrimaryKey;
public class TourPrimaryKeyImpl implements TourPrimaryKey {
    private long id;
    public long getId() { return this.id; }
    public void setId(long id) { this.id = id; }
}