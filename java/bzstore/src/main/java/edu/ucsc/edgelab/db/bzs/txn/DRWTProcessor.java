package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterClient;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterConnector;
import edu.ucsc.edgelab.db.bzs.data.LockManager;
import edu.ucsc.edgelab.db.bzs.data.TransactionCache;
import edu.ucsc.edgelab.db.bzs.performance.PerfMetricManager;
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
    private PerfMetricManager perfMetricManager;

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
        long startTime = System.currentTimeMillis();
        TxnUtils txnUtils = new TxnUtils();
        ClusterClient clusterClient = ClusterConnector.getClusterClientInstance();
        String batchID = String.format("%d:%d", cid.intValue(), epochNumber.intValue());
        Bzs.TransactionBatch prepareBatch = txnUtils.getTransactionBatch(batchID, txns.values(), Bzs.Operation.DRWT_PREPARE);

        logger.info(String.format("DRWT Prepare batch: %s", prepareBatch.getID()));
        Bzs.TransactionBatchResponse batchResponse = clusterClient.execute(ClusterClient.DRWT_Operations.PREPARE_BATCH, prepareBatch, cid);


        DependencyVectorManager.updateLocalClock(batchResponse.getDepVectorMap());
        logger.info("DRWTProcessor: Prepare Batch Response: " + batchResponse.getID());

        Set<TransactionID> abortSet = new LinkedHashSet<>();
        for (Bzs.TransactionResponse response : batchResponse.getResponsesList()) {
            TransactionID tid = TransactionID.getTransactionID(response.getTransactionID());
            if (!response.getStatus().equals(Bzs.TransactionStatus.PREPARED)) {
                logger.info("Transaction was not prepared: " + response);
                txnUtils.sendAbortToClient(response, tid);
                Bzs.Transaction txn = txns.remove(tid);
                TransactionCache.removeHistory(tid);
                LockManager.releaseLocks(txn);
                abortSet.add(tid);
            } else {
                txnUtils.updateDRWTxnResponse(response, tid, cid);
            }
        }
        DTxnCache.addToAbortQueue(epochNumber, abortSet);


        Bzs.TransactionBatch commitBatch = txnUtils.getTransactionBatch(batchID, txns.values(), Bzs.Operation.DRWT_COMMIT);
        logger.info("Committing the following  batch: " + commitBatch.getID());
        Bzs.TransactionBatchResponse commitResponse = clusterClient.execute(ClusterClient.DRWT_Operations.COMMIT_BATCH, commitBatch, cid);
        logger.info("Response for commitAll: "+ commitResponse.getID());

        long processingTime = System.currentTimeMillis() - startTime;
        perfMetricManager.insertDTxnPerfData(Epoch.getEpochNumber(), processingTime, txns.size());

        DTxnCache.addToCompletedQueue(epochNumber, txns.keySet());
//        TxnUtils.releaseLocks(commitResponse);

    }

    public void setPerfMetricManager(PerfMetricManager perfLogger) {
        this.perfMetricManager = perfLogger;
    }
}
