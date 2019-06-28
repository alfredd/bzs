package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterClient;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterConnector;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

import java.util.Map;

public class DRWTProcessor implements Runnable{

    private final Integer cid;
    private final Map<TransactionID, Bzs.Transaction> txns;
    private final Integer epochNumber;

    public DRWTProcessor(final Integer epochNumber, final Integer clusterID, final Map<TransactionID, Bzs.Transaction> transactions) {
        this.cid = clusterID;
        this.txns = transactions;
        this.epochNumber = epochNumber;
    }

    /**
     * TODO Connect to cluster with ClusterID. Send a message with the transactions to be committed to remote cluster.
     * Wait for response for transaction response. Update the DTxnCache with the response.
     * Send Abort message to clients and clusters if a transaction fails.
     */
    @Override
    public void run() {
        ClusterClient clusterClient = ClusterConnector.getClusterClientInstance();
        TxnUtils.getTransactionBatch(cid.toString(), txns.values(), Bzs.Operation.BFT_PREPARE);
//        clusterClient.prepareAll(cid, epochNumber, txns.values());
    }
}
