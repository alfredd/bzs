package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.TransactionCache;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;
import io.grpc.stub.StreamObserver;

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
            batchBuilder = batchBuilder.addTransactions(transaction);
        }
        return batchBuilder.setID(batchID).setOperation(operation).build();
    }

    public static Map<Integer, Map<TransactionID, Bzs.Transaction>> mapTransactionsToCluster(final Map<TransactionID, Bzs.Transaction> dRWTs, final int myClusterID) {
        Map<Integer, Map<TransactionID, Bzs.Transaction>> tMap = new TreeMap<>();
        for (Map.Entry<TransactionID, Bzs.Transaction> drwt : dRWTs.entrySet()) {
            if (drwt != null) {
                Set<Integer> cids = getListOfClusterIDs(drwt.getValue(), myClusterID);
                for (Integer cid : cids) {
                    if (!tMap.containsKey(cid)) {
                        tMap.put(cid, new LinkedHashMap<>());
                    }
                    tMap.get(cid).put(drwt.getKey(), drwt.getValue());
                }
            }
        }
        return tMap;
    }

    public static void sendAbortToClient(Bzs.TransactionResponse txnResponse, TransactionID transactionID) {
        StreamObserver<Bzs.TransactionResponse> responseObserver = TransactionCache.getObserver(transactionID);
        responseObserver.onNext(txnResponse);
        responseObserver.onCompleted();
    }
}
