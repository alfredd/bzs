package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DTxnCache {
    private static LinkedList<Integer> epochQueue = new LinkedList<>();
    private static TreeSet<Integer> completedEpochs = new TreeSet<>();
    private static Map<Integer, CacheKeeper> txnCache = new ConcurrentHashMap<>();
    public static boolean log_debug_flag = false;
    public static boolean statusHistory = true;

    private static final Logger logger = Logger.getLogger(DTxnCache.class.getName());

    private static void addEpochToQueue(Integer epochNumber) {
        epochQueue.addLast(epochNumber);
    }

    public static Collection<Bzs.Transaction> getCommittedTransactions() {
//        logger.info("Returning committed transactions.");
        Set<Bzs.Transaction> committedTxns = new LinkedHashSet<>();
        try {

            while (completedDRWTxnsExist()) {
                Integer head = epochQueue.getFirst();
                if (head != null) {
                    CacheKeeper cache = txnCache.get(head);
                    boolean allCompleted = cache.allCompleted();
                    logger.info("Removing the head of the epoch queue: "+ head+", all Txns Completed? "+ allCompleted);
                    if (allCompleted) {
                        epochQueue.removeFirst();
                        completedEpochs.remove(head);
                        committedTxns.addAll(cache.getCompletedTxns());
                    }
                }
            }
        } catch (Exception e ) {
            logger.log(Level.WARNING, "DEBUG: "+e.getLocalizedMessage(), e);
        }
        return committedTxns;
    }

    public static boolean completedDRWTxnsExist() {
        boolean status = false;
        if (epochQueue.size()>0) {
            final Integer first = epochQueue.getFirst();
            status = completedEpochs.size() > 0 && completedEpochs.contains(first);
        }

        if (log_debug_flag) {
            if (status != statusHistory)
                logger.info("Complexted DRWTxns exists? " + status);
            statusHistory= status;

//            logger.info("Epoch Queue: "+epochQueue);
        }
        return status;
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
        log_debug_flag = true;
        logger.info(String.format("Adding transactions to txnCache for epoch: %d", epochNumber.intValue()/*, completed.toString()*/));
        CacheKeeper cache = txnCache.get(epochNumber);
        cache.addToCompleted(completed);
//        logger.info("Remaining TIDs to be committed: "+ cache.getRemaining());
        if (cache.allCompleted()) {
            completedEpochs.add(epochNumber);
        }
        logger.info("Completed Epochs: "+ completedEpochs);
        logger.info("Epoch queue: "+ epochQueue);

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
    private Set<TransactionID> inProgressTxnMap = new LinkedHashSet<>();
    private Set<Bzs.Transaction> transactions = new LinkedHashSet<>();

    private static final Logger logger = Logger.getLogger(CacheKeeper.class.getName());

    public void addToCompleted(Collection<TransactionID> completed) {
        for (TransactionID tid : completed)
            inProgressTxnMap.remove(tid);
    }

    public Set<TransactionID> getRemaining() {
        return inProgressTxnMap;
    }

    public void addToInProgress(final Map<TransactionID, Bzs.Transaction> transactions) {
        inProgressTxnMap.addAll(transactions.keySet());
        this.transactions.addAll(transactions.values());
    }

    public void addToAborted(Set<TransactionID> abortSet) {
        addToCompleted(abortSet);
    }

    public boolean allCompleted() {
        return inProgressTxnMap.size() == 0;
    }

    public Set<Bzs.Transaction> getCompletedTxns() {
        return this.transactions;
    }
}