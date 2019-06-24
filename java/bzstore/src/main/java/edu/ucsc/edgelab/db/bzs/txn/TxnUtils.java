package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.TransactionCache;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

import java.util.*;
import java.util.logging.Logger;

public class TxnUtils {

    private static final Logger logger = Logger.getLogger(TxnUtils.class.getName());

    public static Set<Integer> getListOfClusterIDs(final Bzs.Transaction remoteTransaction, final Integer cid) {

        Set<Integer> cidSet = new HashSet<>();

        for (int i = 0; i < remoteTransaction.getReadHistoryCount(); i++) {
            int clusterID = remoteTransaction.getReadHistory(i).getClusterID();
            addToCidSet(cidSet, clusterID, cid);
        }
        for (int i = 0; i < remoteTransaction.getWriteOperationsCount(); i++) {
            int clusterID = remoteTransaction.getWriteOperations(i).getClusterID();
            addToCidSet(cidSet, clusterID, cid);
        }
        logger.info("Set of CIDs to which the request will be sent: " + cidSet);
        return cidSet;
    }

    private static void addToCidSet(Set<Integer> cidSet, int clusterID, int myCid) {
        if (clusterID != myCid)
            cidSet.add(clusterID);
    }

    public static Bzs.TransactionBatch getTransactionBatch(final String batchID, final Collection<Bzs.Transaction> transactions, final Bzs.Operation operation) {
        Bzs.TransactionBatch.Builder batchBuilder = Bzs.TransactionBatch.newBuilder();

        for (Bzs.Transaction transaction : transactions) {
            batchBuilder.addTransactions(transaction);
        }
        batchBuilder.setID(batchID).setOperation(operation);
        return batchBuilder.build();
    }

    public static Map<Integer, List<Bzs.Transaction>> mapTransactionsToCluster(final Set<TransactionID> dRWTs, final int myClusterID) {
        Map<Integer, List<Bzs.Transaction>> tMap = new TreeMap<>();
        for (TransactionID dRWTid : dRWTs) {
            Bzs.Transaction drwt = TransactionCache.getTransaction(dRWTid);
            if (drwt != null) {
                Set<Integer> cids = getListOfClusterIDs(drwt, myClusterID);
                for (Integer cid : cids) {
                    if (!tMap.containsKey(cid)) {
                        tMap.put(cid, new LinkedList<>());
                    }
                    tMap.get(cid).add(drwt);
                }
            }
        }
        return tMap;
    }
}
