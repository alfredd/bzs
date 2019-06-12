package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Smr {
    private Smr() {}

    private SmrLog smrLog = new SmrLog();


}

class SmrLog {

    public static final Logger log = Logger.getLogger(SmrLog.class.getName());

    private Map<Integer, Bzs.SmrLogEntry.Builder> smrLog = new ConcurrentHashMap<>();

    void createLogEntry(final Integer epochNumber) {
        if (!smrLog.containsKey(epochNumber)) {
            Bzs.SmrLogEntry.Builder logEntryBuilder = Bzs.SmrLogEntry.newBuilder();
            logEntryBuilder.setEpochNumber(epochNumber);
            smrLog.put(epochNumber, logEntryBuilder);
        } else {
            log.log(Level.WARNING, "Log entry already exists for "+ epochNumber);
        }
    }

    void add(TransactionID tid, Bzs.Transaction transaction) {
    }

    void localPrepare (TransactionID tid) {
    }

    void distributedPrepare(TransactionID tid) {

    }

    void addToCommitLog(TransactionID tid) {

    }

    void commitEpoch(int epochNumber) {

    }
}