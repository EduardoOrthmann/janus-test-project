package com.tsystems.dqm.tmgmt.beladeliste.interfaces;

import java.util.List;

public interface Sendung {
    List<Lieferschein> getLieferscheine();
    void setLieferscheine(List<Lieferschein> lieferscheine);
}