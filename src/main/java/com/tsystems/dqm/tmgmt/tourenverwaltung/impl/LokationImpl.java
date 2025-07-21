package com.tsystems.dqm.tmgmt.tourenverwaltung.impl;
import com.tsystems.dqm.tmgmt.tourenverwaltung.interfaces.*;
import java.util.List;
public class LokationImpl implements Lokation {
    private long id;
    public long getId() { return this.id; }
    public void setId(long id) { this.id = id; }
    public void setWerknummer(String w) {}
    public void setName(String n) {}
    public void setZielArt(String a) {}
    public void setWESZeitstelle(String z) {}
    public void setWESSegmentNr(String s) {}
    public void setKoordinaten(List<Koordinaten> k) {}
}