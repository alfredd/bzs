package edu.ucsc.edgelab.db.bzs.replica;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SmrLog {

    private static final Logger log = Logger.getLogger(SmrLog.class.getName());

    private static final Map<Integer, SMRData> smrEpochData = new ConcurrentHashMap<>();


    public static void createLogEntry(final Integer epochNumber) {
        if (!smrEpochData.containsKey(epochNumber)) {
            SMRData smrData = new SMRData();
            smrData.epoch = epochNumber;
            smrEpochData.put(epochNumber, smrData);
        } else {
            log.log(Level.WARNING, "Log entry already exists for " + epochNumber);
        }
    }

    public static void localPrepared(final Integer epoch, Set<TransactionID> lRWTtids) {
        SMRData smrData = smrEpochData.get(epoch);
        if (smrData!=null) {
            smrData.lRWTs.addAll(lRWTtids);
        }
    }

    public static void distributedPrepared(final Integer epoch, Set<TransactionID> dRWTtids) {
        SMRData smrData = smrEpochData.get(epoch);
        if (smrData!=null) {
            smrData.preparedDRWTs.addAll(dRWTtids);
        }
    }

    public static void committed(final Integer epoch, TransactionID tid) {
        SMRData smrData = smrEpochData.get(epoch);
        if (smrData!=null) {
            smrData.committedDRWTs.add(tid);
        }
    }

    public static void dependencyVector (final Integer epoch, List<Integer> dvec) {
        SMRData smrData = smrEpochData.get(epoch);
        if (smrData!=null) {
            smrData.dependencyVector = dvec;
        }
    }

    public static void lastCommittedEpoch(final Integer epoch,final Integer lastCommittedEpoch) {
        SMRData smrData = smrEpochData.get(epoch);
        if (smrData!=null) {
            smrData.lastCommittedEpoch = lastCommittedEpoch;
        }
    }

//    void commitEpoch(Integer epochNumber) {
//        Bzs.SmrLogEntry.Builder epochBlockBuilder = smrLog.get(epochNumber);
//
//        Bzs.SmrLogEntry logEntry = epochBlockBuilder.build();
//        try {
//            BZDatabaseController.commitEpochBlock(epochNumber.toString(), logEntry);
//            smrLog.remove(epochNumber);
//        } catch (InvalidCommitException e) {
//            log.log(Level.WARNING, "Could not commit smrLogEntry to database: " + e.getLocalizedMessage());
//        }
//    }
}

