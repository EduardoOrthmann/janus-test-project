package com.tsystems.dqm.tmgmt.tourenverwaltung.interfaces;

import java.util.List;

public interface Lokation {
    long getId();
    void setId(long id);
    void setWerknummer(String werk);
    void setName(String name);
    void setZielArt(String art);
    void setWESZeitstelle(String zeitstelle);
    void setWESSegmentNr(String segment);
    void setKoordinaten(List<Koordinaten> koordinaten);
}