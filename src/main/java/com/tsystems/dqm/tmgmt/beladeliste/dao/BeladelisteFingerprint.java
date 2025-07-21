package com.tsystems.dqm.tmgmt.beladeliste.dao;

import java.util.Collections;
import java.util.Iterator;

public class BeladelisteFingerprint {
    public void addLine(long transportId, long sendingId) {}
    public Iterator<FingerprintLine> getLines() { return Collections.emptyIterator(); }

    public static class FingerprintLine {
        public long getTransportID() { return 0L; }
        public long getSendingID() { return 0L; }
    }
}