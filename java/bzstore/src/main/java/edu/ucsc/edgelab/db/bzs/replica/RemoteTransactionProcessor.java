package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;

public class RemoteTransactionProcessor  {

    public Integer clusterID;
    public Integer replicaID;
    private TransactionProcessor processor;

    public RemoteTransactionProcessor(Integer clusterID, Integer replicaID) {
        this.clusterID = clusterID;
        this.replicaID = replicaID;
    }

    public void processAsync(TransactionID tid, Bzs.Transaction request) {
        // Create a new thread for each tid
        // Get remote cluster ID
        // create a map of transaction=>clusterid=>status of prepare message
        // once responses of prepared messages start coming back, update the transaction status.
        // Once all responses are received call processor.prepared(tid)
    }

    public void setObserver(TransactionProcessor transactionProcessor) {
        this.processor = transactionProcessor;
    }
}
