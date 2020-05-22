package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterClient;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterConnector;
import edu.ucsc.edgelab.db.bzs.performance.PerfMetricManager;
import edu.ucsc.edgelab.db.bzs.replica.ID;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

import java.util.*;
import java.util.logging.Logger;

public class DRWTCommitWorker implements Runnable {

    private final Integer cid;
    private Map<Integer, List<Bzs.TransactionResponse>> committedTxnsMap;
    public static final Logger logger = Logger.getLogger(DRWTCommitWorker.class.getName());
    private PerfMetricManager perfMetricManager;



    public DRWTCommitWorker(Integer clusterID, Map<Integer, List<Bzs.TransactionResponse>> committedTxns) {
        this.cid = clusterID;
        this.committedTxnsMap = committedTxns;
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

        StringBuilder builder = new StringBuilder();
        List<Bzs.TransactionResponse> committedTxnsList = new LinkedList<>();
        for (Map.Entry<Integer, List<Bzs.TransactionResponse>> epochTxnList: committedTxnsMap.entrySet()) {
            builder.append(String.format("%d:%d:%d,", ID.getClusterID(),epochTxnList.getKey(), epochTxnList.getValue().size() ));
            committedTxnsList.addAll(epochTxnList.getValue());
        }
        // TODO is this a unique ID per batch.
        String batchID = builder.toString();
        Bzs.TransactionBatch commitBatch = txnUtils.getTransactionResponseBatch(batchID, committedTxnsList, Bzs.Operation.DRWT_COMMIT);

        logger.info(String.format("DRWT Commit batch: %s", commitBatch.getID()));
        Bzs.TransactionBatchResponse batchResponse = clusterClient.execute(ClusterClient.DRWT_Operations.COMMIT_BATCH, commitBatch, cid);



        long processingTime = System.currentTimeMillis() - startTime;
        perfMetricManager.insertDTxnPerfData(Epoch.getEpochNumber(), processingTime, committedTxnsMap.size());
        logger.info("DRWT Commit Processing time: " + processingTime);

        logger.info("DRWT Commit Completed, duration: " + (System.currentTimeMillis()-startTime));
    }

    public void setPerfMetricManager(PerfMetricManager perfLogger) {
        this.perfMetricManager = perfLogger;
    }
}
