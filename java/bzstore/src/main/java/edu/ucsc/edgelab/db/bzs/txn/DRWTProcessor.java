package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterClient;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterConnector;
import edu.ucsc.edgelab.db.bzs.data.LockManager;
import edu.ucsc.edgelab.db.bzs.data.TransactionCache;
import edu.ucsc.edgelab.db.bzs.replica.DependencyVectorManager;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

public class DRWTProcessor implements Runnable {

    private final Integer cid;
    private Map<TransactionID, Bzs.Transaction> txns;
    private final Integer epochNumber;
    public static final Logger logger = Logger.getLogger(DRWTProcessor.class.getName());

    public DRWTProcessor(final Integer epochNumber, final Integer clusterID, final Map<TransactionID, Bzs.Transaction> transactions) {
        this.cid = clusterID;
        this.txns = transactions;
        this.epochNumber = epochNumber;
    }

    /**
     * TODO Update the DTxnCache with the response.
     * Send Abort message to clients and clusters if a transaction fails.
     */
    @Override
    public void run() {
        ClusterClient clusterClient = ClusterConnector.getClusterClientInstance();
        String batchID = String.format("%d:%d", cid.intValue(), epochNumber.intValue());
        Bzs.TransactionBatch prepareBatch = TxnUtils.getTransactionBatch(batchID, txns.values(), Bzs.Operation.DRWT_PREPARE);

        logger.info(String.format("DRWT Prepare batch: %s", prepareBatch));
        Bzs.TransactionBatchResponse batchResponse = clusterClient.execute(ClusterClient.DRWT_Operations.PREPARE_BATCH, prepareBatch, cid);


        DependencyVectorManager.updateLocalClock(batchResponse.getDepVectorMap());
        logger.info("DRWTProcessor: Prepare Batch Response: " + batchResponse);

        Set<TransactionID> abortSet = new LinkedHashSet<>();
        for (Bzs.TransactionResponse response : batchResponse.getResponsesList()) {
            TransactionID tid = TransactionID.getTransactionID(response.getTransactionID());
            if (!response.getStatus().equals(Bzs.TransactionStatus.PREPARED)) {
                logger.info("Transaction was not prepared: " + response);
                TxnUtils.sendAbortToClient(response, tid);
                Bzs.Transaction txn = txns.remove(tid);
                TransactionCache.removeHistory(tid);
                LockManager.releaseLocks(txn);
                abortSet.add(tid);
            }
        }
        DTxnCache.addToAbortQueue(epochNumber, abortSet);

        Bzs.TransactionBatch commitBatch = TxnUtils.getTransactionBatch(batchID, txns.values(), Bzs.Operation.DRWT_COMMIT);
        logger.info("Committing the following  batch: " + commitBatch);
        Bzs.TransactionBatchResponse commitResponse = clusterClient.execute(ClusterClient.DRWT_Operations.COMMIT_BATCH, commitBatch, cid);
        DTxnCache.addToCompletedQueue(epochNumber, txns.keySet());
        releaseLocks(commitResponse);
    }

    private void releaseLocks(Bzs.TransactionBatchResponse batchResponse) {
        for (Bzs.TransactionResponse response : batchResponse.getResponsesList()) {
            TransactionID tid = TransactionID.getTransactionID(response.getTransactionID());
            LockManager.releaseLocks(TransactionCache.getTransaction(tid));
        }
    }
}
