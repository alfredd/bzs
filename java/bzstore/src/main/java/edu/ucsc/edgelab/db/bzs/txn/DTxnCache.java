package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DTxnCache {
    private static List<Integer> epochQueue = new LinkedList<>();
    private static Map<Integer, CacheKeeper> txnCache = new ConcurrentHashMap<>();

    public static final Logger logger = Logger.getLogger(DTxnCache.class.getName());

    private static void addEpochToQueue(Integer epochNumber) {
        epochQueue.add(epochNumber);
    }

    public static void addToInProgressQueue(final Integer epochNumber,
                                            final Map<TransactionID, Bzs.Transaction> transactions) {
        if (!txnCache.containsKey(epochNumber)) {
            txnCache.put(epochNumber, new CacheKeeper());
            addEpochToQueue(epochNumber);
        }
        CacheKeeper cache = txnCache.get(epochNumber);
        cache.addToInProgress(transactions);
        txnCache.put(epochNumber, cache);
    }

    public static void addToCompletedQueue(final Integer epochNumber, final Collection<TransactionID> completed) {
        if (!txnCache.containsKey(epochNumber)) {
            logger.log(Level.WARNING, String.format("No transactions available for epoch: %d.", epochNumber.intValue()));
            return;
        }
        CacheKeeper cache = txnCache.get(epochNumber);
        cache.addToCompleted(completed);

    }

    public static void addToAbortQueue(Integer epochNumber, Set<TransactionID> abortSet) {
        if (!txnCache.containsKey(epochNumber)) {
            logger.log(Level.WARNING, String.format("No transactions available for epoch: %d.", epochNumber.intValue()));
            return;
        }
        CacheKeeper cache = txnCache.get(epochNumber);
        cache.addToAborted(abortSet);
    }
}

class CacheKeeper {
    private Map<TransactionID, Bzs.Transaction> inProgressTxnMap = new LinkedHashMap<>();
    private List<Integer> completedQueue = new LinkedList<>();

    public void addToCompleted(Collection<TransactionID> completed) {
        for (TransactionID tid : completed)
            inProgressTxnMap.remove(tid);
    }

    public void addToInProgress(final Map<TransactionID, Bzs.Transaction> transactions) {
        inProgressTxnMap.putAll(transactions);
    }

    public void addToAborted(Set<TransactionID> abortSet) {
        addToCompleted(abortSet);
    }

    public boolean allCompleted() {
        return inProgressTxnMap.size()==0;
    }
}