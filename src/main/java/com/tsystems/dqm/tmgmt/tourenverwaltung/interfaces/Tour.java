package com.tsystems.dqm.tmgmt.tourenverwaltung.interfaces;

import java.util.Date;

public interface Tour {
    long getId();
    void setId(long id);
    TourPrimaryKey getPrimaryKey();
    void setPrimaryKey(TourPrimaryKey key);
    Lokation getLokation();
    void setLokation(Lokation lokation);
    String getTimeframeGroup();
    void setTimeframeGroup(String group);
    void setBorderonummer(String bordero);
    void setETA1(Date eta1);
    void setETA2(Date eta2);
    void setVerzoegerung(long delay);
    void setStatus(int status);
    void setZeitfensterStart(Date start);
    void setZeitfensterEnde(Date end);
    void setTechnologie(String technologie);
    void setCreationTime(Date date);
}