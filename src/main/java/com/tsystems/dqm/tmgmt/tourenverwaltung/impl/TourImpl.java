package com.tsystems.dqm.tmgmt.tourenverwaltung.impl;
import com.tsystems.dqm.tmgmt.tourenverwaltung.interfaces.*;
import java.util.Date;
public class TourImpl implements Tour {
    private long id;
    private TourPrimaryKey primaryKey;
    private Lokation lokation;
    public long getId() { return this.id; }
    public void setId(long id) { this.id = id; }
    public TourPrimaryKey getPrimaryKey() { return this.primaryKey; }
    public void setPrimaryKey(TourPrimaryKey key) { this.primaryKey = key; }
    public Lokation getLokation() { return this.lokation; }
    public void setLokation(Lokation lokation) { this.lokation = lokation; }
    public String getTimeframeGroup() { return null; }
    public void setTimeframeGroup(String group) {}
    public void setBorderonummer(String b) {}
    public void setETA1(Date d) {}
    public void setETA2(Date d) {}
    public void setVerzoegerung(long l) {}
    public void setStatus(int s) {}
    public void setZeitfensterStart(Date d) {}
    public void setZeitfensterEnde(Date d) {}
    public void setTechnologie(String t) {}
    public void setCreationTime(Date d) {}
}