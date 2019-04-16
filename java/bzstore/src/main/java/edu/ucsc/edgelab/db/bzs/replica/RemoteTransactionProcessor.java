package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterConnector;

import java.util.Timer;

import static edu.ucsc.edgelab.db.bzs.replica.EpochManager.getEpochTimeInMS;

public class RemoteTransactionProcessor {

    public Integer clusterID;
    public Integer replicaID;
    private TransactionProcessor processor;
    private ClusterConnector clusterConnector;

    public RemoteTransactionProcessor(Integer clusterID, Integer replicaID) {
        this.clusterID = clusterID;
        this.replicaID = replicaID;

        Timer epochTimer = new Timer("TimedEpochMaintainer", true);
        Integer epochTimeInMS = getEpochTimeInMS();
        clusterConnector = new ClusterConnector(this.clusterID);
        epochTimer.scheduleAtFixedRate(clusterConnector, 0, epochTimeInMS * 1000 * 10);
    }

    /**
     * The process roughly is:
     * 1. Create a new thread for each tid
     * 2. Get remote cluster ID
     * 3. create a map of transaction=>clusterid=>status of prepare message
     * 4. once responses of remoteOperationObserver messages start coming back, update the transaction status.
     * 5. Once all responses are received call processor.remoteOperationObserver(tid)
     * This is done by the @{@link PrepareProcessor}
     *
     * @param tid
     * @param request
     */
    public void prepareAsync(final TransactionID tid, final Bzs.Transaction request) {

        runProcessor(new PrepareProcessor(tid, request, clusterID, replicaID, clusterConnector));
    }

    private void runProcessor(RemoteOpProcessor remoteProcessor) {
        remoteProcessor.setResponseObserver(processor);
        new Thread(remoteProcessor).start();
    }

    public void commitAsync(final TransactionID tid, final Bzs.Transaction request) {
        runProcessor(new CommitProcessor(clusterID, replicaID, tid, request, clusterConnector));
    }

    public void setObserver(TransactionProcessor transactionProcessor) {
        this.processor = transactionProcessor;
    }
}