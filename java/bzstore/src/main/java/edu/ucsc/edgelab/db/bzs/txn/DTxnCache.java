package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.performance.BatchMetricsManager;
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
    private static BatchMetricsManager batchMetricsManagerObj;

    private static void addEpochToQueue(Integer epochNumber) {
        epochQueue.addLast(epochNumber);
    }

    public static Set<Bzs.TransactionResponse> getCommittedTransactions() {
//        logger.info("Returning committed transactions.");
        Set<Bzs.TransactionResponse> committedTxns = new LinkedHashSet<>();
        try {

            while (completedDRWTxnsExist()) {
                Integer head = epochQueue.getFirst();
                if (head != null) {
                    CacheKeeper cache = txnCache.get(head);
                    boolean allCompleted = cache.allCompleted();
                    logger.info("Removing the head of the epoch queue: " + head + ", all Txns Completed? " + allCompleted);
                    if (allCompleted) {
                        epochQueue.removeFirst();
                        completedEpochs.remove(head);
                        committedTxns.addAll(cache.getPreparedTxns());
                    }
                }
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "DEBUG: " + e.getLocalizedMessage(), e);
        }
        return committedTxns;
    }

    public static boolean completedDRWTxnsExist() {
        boolean status = false;
        if (epochQueue.size() > 0) {
            final Integer first = epochQueue.getFirst();
            status = completedEpochs.size() > 0 && completedEpochs.contains(first);
        }

        if (log_debug_flag) {
//            if (status != statusHistory)
//                logger.info("Complexted DRWTxns exists? " + status);
            statusHistory = status;

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

    public static void addToCompletedQueue(final Integer epochNumber, final Collection<TransactionID> completed, String remoteBatchID,
                                           List<Bzs.TransactionResponse> preparedResponse) {
        if (!txnCache.containsKey(epochNumber)) {
            logger.log(Level.WARNING, String.format("No transactions available for epoch: %d.", epochNumber.intValue()));
            return;
        }
        log_debug_flag = true;
//        logger.info(String.format("Adding transactions to txnCache for epoch: %d", epochNumber.intValue()/*, completed.toString()*/));
        CacheKeeper cache = txnCache.get(epochNumber);
        cache.addToCompleted(preparedResponse, remoteBatchID);
        for (TransactionID tid : completed)
            batchMetricsManagerObj.setTxnProcessingCompleted(tid);
//        logger.info("Remaining TIDs to be committed: "+ cache.getRemaining());
        if (cache.allCompleted()) {
            completedEpochs.add(epochNumber);
        }
//        logger.info("Completed Epochs: "+ completedEpochs);
//        logger.info("Epoch queue: "+ epochQueue);

    }

    public static void addToAbortQueue(Integer epochNumber, Set<TransactionID> abortSet, String remoteBatchID) {
        if (!txnCache.containsKey(epochNumber)) {
            logger.log(Level.WARNING, String.format("No transactions available for epoch: %d.", epochNumber.intValue()));
            return;
        }
        CacheKeeper cache = txnCache.get(epochNumber);
        cache.addToAborted(abortSet, remoteBatchID);
    }

    public static void setEpochMetricsManager(BatchMetricsManager batchMetricsManager) {
        batchMetricsManagerObj = batchMetricsManager;
    }
}

class CacheKeeper {
    private Set<TransactionID> inProgressTxnMap = new LinkedHashSet<>();
    private Map<TransactionID, Integer> tidWriteOpCountMap = new ConcurrentHashMap<>();
    private Set<Bzs.Transaction> transactions = new LinkedHashSet<>();
    private Map<TransactionID, Bzs.TransactionResponse> preparedTxns = new ConcurrentHashMap<>();
//    private Map<String, List<Bzs.TransactionResponse>> preparedTxnResponses = new ConcurrentHashMap<>();
//    private Map<String, Set<TransactionID>> remoteTxnBatchMap = new ConcurrentHashMap<>();

    private static final Logger logger = Logger.getLogger(CacheKeeper.class.getName());

    public void addToCompleted(final List<Bzs.TransactionResponse> preparedResponses, final String remoteBatchID) {
//        if (!remoteTxnBatchMap.containsKey(remoteBatchID)) {
//            remoteTxnBatchMap.put(remoteBatchID, new HashSet<>());
//        }
//        if (!preparedTxnResponses.containsKey(remoteBatchID))
//            preparedTxnResponses.put(remoteBatchID, new LinkedList<>());

        for (Bzs.TransactionResponse preparedResponse : preparedResponses) {
            TransactionID tid = TransactionID.getTransactionID(preparedResponse.getTransactionID());
            if (!preparedTxns.containsKey(tid)) {
                preparedTxns.put(tid, preparedResponse);
            } else {
                Bzs.TransactionResponse resp = preparedTxns.get(tid);
                Bzs.TransactionResponse modResp =
                        Bzs.TransactionResponse.newBuilder(resp).addAllWriteResponses(preparedResponse.getWriteResponsesList()).build();
                preparedTxns.put(tid, modResp);
            }

            Integer opCount = tidWriteOpCountMap.get(tid);
            opCount -= preparedResponse.getWriteResponsesCount();
            tidWriteOpCountMap.put(tid, opCount);
            if (opCount == 0) {
                inProgressTxnMap.remove(tid);

/*                synchronized (remoteBatchID) {
                    List<Bzs.TransactionResponse> list = preparedTxnResponses.get(remoteBatchID);
                    list.add(preparedResponse);
                    preparedTxnResponses.put(remoteBatchID, list);
                }*/
            }
//            remoteTxnBatchMap.get(remoteBatchID).add(tid);
        }
    }

    public void addToInProgress(final Map<TransactionID, Bzs.Transaction> transactions) {
        inProgressTxnMap.addAll(transactions.keySet());
        Collection<Bzs.Transaction> txns = transactions.values();
        for (Bzs.Transaction t : txns) {
            TransactionID tid = TransactionID.getTransactionID(t.getTransactionID());
            tidWriteOpCountMap.put(tid, t.getWriteOperationsCount());
        }

// No Need to store transactions. Only prepared responses need to be stored.
//        this.transactions.addAll(transactions.values());
    }

    public void addToAborted(Set<TransactionID> abortSet, String remoteBatchID) {
        for (TransactionID tid : abortSet) {
            if (inProgressTxnMap.contains(tid))
                inProgressTxnMap.remove(tid);
            if (tidWriteOpCountMap.containsKey(tid))
                tidWriteOpCountMap.remove(tid);
            if (preparedTxns.containsKey(tid))
                preparedTxns.remove(tid);
        }
    }

    public boolean allCompleted() {
        return inProgressTxnMap.size() == 0;
    }

    public Collection<Bzs.TransactionResponse> getPreparedTxns() {
        return this.preparedTxns.values();
    }
}