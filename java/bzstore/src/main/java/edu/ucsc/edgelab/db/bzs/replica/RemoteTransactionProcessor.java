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
     *    Create a new thread for each tid
     *    Get remote cluster ID
     *    create a map of transaction=>clusterid=>status of prepare message
     *    once responses of prepared messages start coming back, update the transaction status.
     *    Once all responses are received call processor.prepared(tid)
     * @param tid
     * @param request
     */
    public void processAsync(final TransactionID tid, final Bzs.Transaction request) {

        RemoteProcessor remoteProcessor = new RemoteProcessor(tid,request,clusterID,replicaID, clusterConnector);
        remoteProcessor.setResponseObserver(processor);
        new Thread(remoteProcessor).start();
    }

    public void setObserver(TransactionProcessor transactionProcessor) {
        this.processor = transactionProcessor;
    }
}