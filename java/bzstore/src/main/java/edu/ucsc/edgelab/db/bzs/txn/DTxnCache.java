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

    public static void addToInProgressQueue(final Integer epochNumber, final Integer clusterID,
                                            final Map<TransactionID, Bzs.Transaction> transactions) {
        if (!txnCache.containsKey(epochNumber)) {
            txnCache.put(epochNumber, new CacheKeeper());
            addEpochToQueue(epochNumber);
        }
        CacheKeeper cache = txnCache.get(epochNumber);
        cache.addToInProgress(clusterID, transactions);
        txnCache.put(epochNumber, cache);
    }

    public static void addToCompletedQueue(final Integer epochNumber, final Collection<TransactionID> completed) {
        if (!txnCache.containsKey(epochNumber)) {
            logger.log(Level.WARNING, String.format("No transactions available for epoch: %d.", epochNumber.intValue()));
            return;
        }
        CacheKeeper cache = txnCache.get(epochNumber);
        cache.addToCompleted(epochNumber, completed);

    }

}

class CacheKeeper {
    private Map<Integer, Map<TransactionID, Bzs.Transaction>> inProgressTxnMap = new LinkedHashMap<>();

    private List<Integer> completedQueue = new LinkedList<>();
    boolean isCompleted = true;

    public void addToCompleted(final Integer epochNumber, Collection<TransactionID> completed) {
        Map<TransactionID, Bzs.Transaction> txnMap = inProgressTxnMap.get(epochNumber);
        for (TransactionID tid : completed)
            txnMap.remove(tid);
    }

    public void addToInProgress(final Integer clusterID, final Map<TransactionID, Bzs.Transaction> transactions) {

        inProgressTxnMap.put(clusterID, transactions);
    }
}