package com.tsystems.dqm.tmgmt.beladeliste.interfaces;

import java.util.List;

public interface Lieferschein {
    String getHerstellernummer();
    void setHerstellernummer(String herstellernummer);
    List<PackstueckPosition> getPackstuecke();
    void setPackstuecke(List<PackstueckPosition> packstuecke);
}