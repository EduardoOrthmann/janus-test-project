package com.tsystems.dqm.tmgmt.tourenverwaltung.interfaces;

import java.util.Date;

public interface BorderoNummer {
    int STATUS_NICHTVORHANDEN = 0;

    void setBorderoNummer(String nummer);
    void setTourId(Long id);
    void setTourStatus(int status);
    void setTimeframeGroup(String group);
    void setTourCreationTime(Date date);
}