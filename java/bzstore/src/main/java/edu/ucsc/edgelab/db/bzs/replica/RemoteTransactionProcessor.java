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

    public void processAsync(TransactionID tid,
                             Bzs.Transaction request) {

    }

    public void setObserver(TransactionProcessor transactionProcessor) {
        this.processor = transactionProcessor;
    }
}
