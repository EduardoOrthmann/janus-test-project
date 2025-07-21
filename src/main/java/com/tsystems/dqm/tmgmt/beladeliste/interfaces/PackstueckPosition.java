package com.tsystems.dqm.tmgmt.beladeliste.interfaces;

import java.util.List;
import java.util.Date;

public interface PackstueckPosition {
    int STATUS_ROT = 1;
    int STATUS_GELB = 2;
    int STATUS_GRUEN = 3;

    String getPackstuecknummer();
    void setPackstuecknummer(String packstuecknummer);
    String getLieferantennummer();
    void setLieferantennummer(String lieferantennummer);
    Date getScanzeitpunkt();
    void setScanzeitpunkt(Date scanzeitpunkt);
    void setStatusInBeladeliste(int status);
    int getStatusInBeladeliste();
    boolean isMaster();
    List<PackstueckPosition> getPackstuecke();
    String getLabelkennung();
    void setLabelkennung(String kennung);
    void setMasterpackstuecknummer(String value);
    void setAbladestelle(String value);
    void setAnzahlPackmittel(String value);
    void setChargennummer(String value);
    void setDublette(short value);
    void setEigentumskennung(String value);
    void setGeometrieBreite(String value);
    void setGeometrieFuellmenge(String value);
    void setGeometrieGewicht(String value);
    void setGeometrieHoehe(String value);
    void setGeometrieLaenge(String value);
    void setGeometrieStapelfaktor(String value);
    void setId(long value);
    void setKennzeichenUlUel(String value);
    void setLadungstraegerpositionsNr(String value);
    void setLagerabrufnummer(String value);
    void setLieferscheinId(long value);
    void setLieferscheinpackstueckRef(String value);
    void setLieferscheinpositionId(long value);
    void setLieferscheinpositionsnummer(String value);
    void setPackmittelnummer(String value);
    void setPackmittelnummerLieferant(String value);
    void setPositionsnummerLieferschein(String value);
    void setSendungId(long value);
    void setStatus(String value);
    void setVerpackungskennung(String value);
    void setPackstuecke(List<PackstueckPosition> packstuecke);
}