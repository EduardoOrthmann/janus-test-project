package com.tsystems.dqm.tmgmt.beladeliste.interfaces;

import java.util.List;
import java.util.Date;

public interface Beladeliste {
    String getWerknummer();
    void setWerknummer(String werknummer);
    String getBorderoNummer();
    void setBorderoNummer(String borderoNummer);
    int getVersion();
    void setVersion(int version);
    void setSpediteurnummer(String spediteurnummer);
    void setKfzKennzeichen(String kfzKennzeichen);
    List<Sendung> getBeladung();
    void setBeladung(List<Sendung> beladung);
    List<PackstueckPosition> getPackstueckeOhneAvis();
    void setPackstueckeOhneAvis(List<PackstueckPosition> packstuecke);
    List<PackstueckPosition> getScannedPackstuecke();
}