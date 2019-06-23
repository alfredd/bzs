package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.txn.Epoch;
import edu.ucsc.edgelab.db.bzs.txn.SmrLogEntryCreator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SmrLog {

    private static final Logger log = Logger.getLogger(SmrLog.class.getName());

    private static final Map<Integer, SmrLogEntryCreator> smrEpochData = new ConcurrentHashMap<>();
    private static int lastLCE = -1;


    public static void setLockLCEForEpoch(int lockLCEForEpoch) {
        SmrLog.lockLCEForEpoch = lockLCEForEpoch;
    }

    private static int lockLCEForEpoch = -1;
    private static Map<Integer, Integer> lceMap = new ConcurrentHashMap<>();


    public static void createLogEntry(final Integer epochNumber) {
        if (!smrEpochData.containsKey(epochNumber)) {
            SmrLogEntryCreator smrData = new SmrLogEntryCreator();
            smrData.setEpochNumber(epochNumber);
            smrData.addLastCommittedEpoch(lastLCE);
            smrEpochData.put(epochNumber, smrData);
        } else {
            log.log(Level.WARNING, "Log entry already exists for " + epochNumber);
        }
    }

    public static SmrLogEntryCreator getSMRData(int epoch) {
        if (!smrEpochData.containsKey(epoch)) {
            createLogEntry(epoch);
        }
        return smrEpochData.get(epoch);
    }

    public static void localPrepared(final Integer epoch, Set<Bzs.Transaction> lRWTtids) {
        SmrLogEntryCreator smrData = smrEpochData.get(epoch);
        if (smrData != null) {
            smrData.addCommittedlWRTxns(lRWTtids);
        }
    }

    public static void distributedPrepared(final Integer epoch, Set<Bzs.Transaction> dRWTtids) {
        SmrLogEntryCreator smrData = smrEpochData.get(epoch);
        if (smrData != null) {
            smrData.addPreparedDRWTxns(dRWTtids);
        }
    }

    public static void committedDRWT(Bzs.Transaction tid) {
        int commitToEpoch = Epoch.getEpochUnderExecution();
        int lce = tid.getEpochNumber();
        if (lockLCEForEpoch == commitToEpoch) {
            commitToEpoch = Epoch.getEpochNumber();
            if (lceMap.containsKey(commitToEpoch)) {
                if (lceMap.get(commitToEpoch) > tid.getEpochNumber())
                    lce = lceMap.get(commitToEpoch);
            }
        }
        lceMap.put(commitToEpoch, lce);
        SmrLogEntryCreator smrData = getSMRData(commitToEpoch);
        if (smrData != null) {
            smrData.addCommittedDRWTxns(tid);
        }
    }

    public static void dependencyVector(final Integer epoch, List<Integer> dvec) {
        SmrLogEntryCreator smrData = smrEpochData.get(epoch);
        if (smrData != null) {
            smrData.addDepVectorToSmrLog(dvec);
        }
    }

    public static void updateLastCommittedEpoch(final Integer epoch) {
        SmrLogEntryCreator smrData = getSMRData(epoch);
        if (smrData != null) {
            Integer lce = lastLCE;
            if (lceMap.containsKey(epoch)) {
                lce = lceMap.get(epoch);
            } else {
                lceMap.put(epoch, lastLCE);
            }
            smrData.addLastCommittedEpoch(lce);
            final Integer lce1 = lceMap.get(epoch);
            if (lce1 > lastLCE)
                lastLCE = lce1;
        }
    }

    public static Bzs.SmrLogEntry generateLogEntry(final Integer epochNumber) {
        SmrLogEntryCreator data = getSMRData(epochNumber);
        return data.generateSmrLogEntry();
    }
}

