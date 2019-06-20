package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.txn.Epoch;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SmrLog {

    private static final Logger log = Logger.getLogger(SmrLog.class.getName());

    private static final Map<Integer, SMRData> smrEpochData = new ConcurrentHashMap<>();


    public static void setLockLCEForEpoch(int lockLCEForEpoch) {
        SmrLog.lockLCEForEpoch = lockLCEForEpoch;
    }

    private static int lockLCEForEpoch = -1;
    private static Map<Integer, Integer> lceMap = new ConcurrentHashMap<>();


    public static void createLogEntry(final Integer epochNumber) {
        if (!smrEpochData.containsKey(epochNumber)) {
            SMRData smrData = new SMRData();
            smrData.epoch = epochNumber;
            smrEpochData.put(epochNumber, smrData);
        } else {
            log.log(Level.WARNING, "Log entry already exists for " + epochNumber);
        }
    }

    public static SMRData getSMRData(int epoch) {
        if (!smrEpochData.containsKey(epoch)) {
            createLogEntry(epoch);
        }
        return smrEpochData.get(epoch);
    }

    public static void localPrepared(final Integer epoch, Set<TransactionID> lRWTtids) {
        SMRData smrData = smrEpochData.get(epoch);
        if (smrData != null) {
            smrData.lRWTs.addAll(lRWTtids);
        }
    }

    public static void distributedPrepared(final Integer epoch, Set<TransactionID> dRWTtids) {
        SMRData smrData = smrEpochData.get(epoch);
        if (smrData != null) {
            smrData.preparedDRWTs.addAll(dRWTtids);
        }
    }

    public static void committedDRWT(TransactionID tid) {
        int commitToEpoch = Epoch.getEpochUnderExecution();
        if (lockLCEForEpoch == commitToEpoch) {
            commitToEpoch = Epoch.getEpochNumber();
            int lce = tid.getEpochNumber();
            if (lceMap.containsKey(commitToEpoch)) {
                if (lceMap.get(commitToEpoch)>tid.getEpochNumber())
                    lce=lceMap.get(commitToEpoch);
            }
            lceMap.put(commitToEpoch, lce);
        }
        SMRData smrData = getSMRData(commitToEpoch);
        if (smrData != null) {
            smrData.committedDRWTs.add(tid);
        }
    }

    public static void dependencyVector(final Integer epoch, List<Integer> dvec) {
        SMRData smrData = smrEpochData.get(epoch);
        if (smrData != null) {
            smrData.dependencyVector = dvec;
        }
    }

    public static void updateLastCommittedEpoch(final Integer epoch) {
        SMRData smrData = smrEpochData.get(epoch);
        if (smrData != null) {
            smrData.lastCommittedEpoch = lceMap.get(epoch);
        }
    }

    public static Bzs.SmrLogEntry generateLogEntry(Integer epochNumber) {
        SMRData data = getSMRData(epochNumber);

        return null;
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

