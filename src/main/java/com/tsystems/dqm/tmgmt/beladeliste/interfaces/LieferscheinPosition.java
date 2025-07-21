package com.tsystems.dqm.tmgmt.beladeliste.interfaces;

public interface LieferscheinPosition {
    void setId(long id);
    long getPackstueckPositionId();
    void setPackstueckPositionId(long id);
    void setSachnummerKunde(String value);
    void setMengeneinheit1(String value);
}