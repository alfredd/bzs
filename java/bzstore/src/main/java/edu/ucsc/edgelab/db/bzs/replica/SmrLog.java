package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.txn.Epoch;
import edu.ucsc.edgelab.db.bzs.txn.SmrLogEntryCreator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SmrLog {

    private static final Logger log = Logger.getLogger(SmrLog.class.getName());

    private static final Map<Integer, SmrLogEntryCreator> smrEpochData = new ConcurrentHashMap<>();
    private static int lastLCE = -1;
    private static int lockLCEForEpoch = -1;
    private static Map<Integer, Integer> lceMap = new ConcurrentHashMap<>();


    public static void updateEpochLCE(Integer epochNumber, int lce) {
        if (!lceMap.containsKey(epochNumber)) {
            lceMap.put(epochNumber, lce);
        } else {
            Integer epochLCE = lceMap.get(epochNumber);
            if (epochLCE< lce)
                lceMap.put(epochNumber, lce);
        }
    }

    public static void setLockLCEForEpoch(int lockLCEForEpoch) {
        SmrLog.lockLCEForEpoch = lockLCEForEpoch;
    }

    public static void createLogEntry(final Integer epochNumber) {
        if (!smrEpochData.containsKey(epochNumber)) {
            SmrLogEntryCreator smrData = new SmrLogEntryCreator();
            smrData.setEpochNumber(epochNumber);
//            smrData.updateLastCommittedEpoch(lastLCE);
            lceMap.put(epochNumber, lastLCE);
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

    public static void localPrepared(final Integer epoch, Collection<Bzs.Transaction> lRWTtids) {
        SmrLogEntryCreator smrData = smrEpochData.get(epoch);
        if (smrData != null) {
            smrData.addCommittedlWRTxns(lRWTtids);
        }
    }

    public static void twoPCPrepared(final Integer epoch, Collection<Bzs.Transaction> twoPCPreparedTxns, String ID) {
        SmrLogEntryCreator smrData = smrEpochData.get(epoch);
        if (smrData != null) {
            smrData.add2PCPrepared(twoPCPreparedTxns, ID);
        }
    }

    public static void twoPCCommitted(final Integer epoch, Collection<Bzs.Transaction> twoPCPreparedTxns, String ID) {
        SmrLogEntryCreator smrData = smrEpochData.get(epoch);
        if (smrData != null) {
            smrData.add2PCCommitted(twoPCPreparedTxns, ID);
        }
    }

    public static void distributedPrepared(final Integer epoch, Collection<Bzs.Transaction> dRWTtids) {
        SmrLogEntryCreator smrData = smrEpochData.get(epoch);
        if (smrData != null) {
            smrData.addPreparedDRWTxns(dRWTtids);
        }
    }

    public static void committedDRWT(int commitToEpoch, Collection<Bzs.Transaction> transactions) {
        if (transactions.size() < 1)
            return;

        int lce = -1;
        for (Bzs.Transaction t: transactions) {
            if (lce<t.getEpochNumber()) {
                lce = t.getEpochNumber();
            }
        }
        updateEpochLCE(commitToEpoch, lce);
//        if (lockLCEForEpoch == commitToEpoch) {
//            commitToEpoch = Epoch.getEpochNumber();
//            if (lceMap.containsKey(commitToEpoch)) {
//                if (lceMap.get(commitToEpoch) > epochNumber) {
//                    lce = lceMap.get(commitToEpoch);
//                }
//            }
//        }
//        lceMap.put(commitToEpoch, lce);
        SmrLogEntryCreator smrData = getSMRData(commitToEpoch);
        if (smrData != null) {
            for (Bzs.Transaction transaction : transactions) {
                smrData.addCommittedDRWTxns(transaction);
            }
        }
    }

    public static void dependencyVector(final Integer epoch, List<Integer> dvec) {
        SmrLogEntryCreator smrData = smrEpochData.get(epoch);
        if (smrData != null) {
            smrData.addDepVectorToSmrLog(dvec);
        }
    }

    public static void dependencyVector(int epoch, Map<Integer, Integer> depVecMap) {
        SmrLogEntryCreator smrData = smrEpochData.get(epoch);
        if (smrData != null) {
            smrData.addDepVecMap(depVecMap);
        }
    }

    public static void updateLastCommittedEpoch(final Integer epoch) {
        SmrLogEntryCreator smrData = getSMRData(epoch);
        if (smrData != null) {
         /*   Integer lce = lastLCE;
            if (lceMap.containsKey(epoch)) {
                lce = lceMap.get(epoch);
            } else {
                lceMap.put(epoch, lastLCE);
            }
            smrData.updateLastCommittedEpoch(lce);
            final Integer lce1 = lceMap.get(epoch);
            if (lce1 > lastLCE)
                lastLCE = lce1;*/
            Integer lce = lceMap.get(epoch);
            lastLCE = lce;
            smrData.updateLastCommittedEpoch(lce);
        }
    }

    public static Bzs.SmrLogEntry generateLogEntry(final Integer epochNumber) {
        SmrLogEntryCreator data = getSMRData(epochNumber);
        return data.generateSmrLogEntry();
    }
}

