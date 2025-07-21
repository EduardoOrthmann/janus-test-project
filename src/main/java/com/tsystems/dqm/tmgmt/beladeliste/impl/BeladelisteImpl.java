package com.tsystems.dqm.tmgmt.beladeliste.impl;

import com.tsystems.dqm.tmgmt.beladeliste.interfaces.*;

import java.util.*;

public class BeladelisteImpl implements Beladeliste {
    public String getWerknummer() {
        return null;
    }

    public void setWerknummer(String werknummer) {
    }

    public String getBorderoNummer() {
        return null;
    }

    public void setBorderoNummer(String borderoNummer) {
    }

    public int getVersion() {
        return 0;
    }

    public void setVersion(int version) {
    }

    public void setSpediteurnummer(String spediteurnummer) {
    }

    public void setKfzKennzeichen(String kfzKennzeichen) {
    }

    public List<Sendung> getBeladung() {
        return Collections.emptyList();
    }

    public void setBeladung(List<Sendung> beladung) {
    }

    public List<PackstueckPosition> getPackstueckeOhneAvis() {
        return null;
    }

    public void setPackstueckeOhneAvis(List<PackstueckPosition> packstuecke) {
    }

    public List<PackstueckPosition> getScannedPackstuecke() {
        return Collections.emptyList();
    }

    public void setPackstueckeJIS(List jisLoadingUnits) {
    }
}