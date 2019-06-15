package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.exceptions.InvalidCommitException;

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

    void localPrepared(TransactionID tid) {
    }

    void distributedPrepared(TransactionID tid) {

    }

    void committed(TransactionID tid) {

    }

    void commitEpoch(Integer epochNumber) {
        Bzs.SmrLogEntry.Builder epochBlockBuilder = smrLog.get(epochNumber);

        Bzs.SmrLogEntry logEntry = epochBlockBuilder.build();
        try {
            BZDatabaseController.commitEpochBlock(epochNumber.toString(), logEntry);
            smrLog.remove(epochNumber);
        } catch (InvalidCommitException e) {
            log.log(Level.WARNING, "Could not commit smrLogEntry to database: "+e.getLocalizedMessage());
        }
    }
}