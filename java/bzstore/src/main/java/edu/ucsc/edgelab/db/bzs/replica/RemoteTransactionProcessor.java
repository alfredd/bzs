package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterConnector;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;

import java.util.Timer;
import java.util.logging.Logger;

public class RemoteTransactionProcessor {

    public Integer clusterID;
    public Integer replicaID;
    private TransactionProcessor processor;
    private ClusterConnector clusterConnector;

    public static final Logger LOG = Logger.getLogger(RemoteTransactionProcessor.class.getName());

    public RemoteTransactionProcessor(Integer clusterID, Integer replicaID) {
        this.clusterID = clusterID;
        this.replicaID = replicaID;

        Integer epochTimeInMS = Configuration.getEpochTimeInMS();
        clusterConnector = new ClusterConnector();
        Timer interClusterConnectorTimer = new Timer("InterClusterConnector", true);
        interClusterConnectorTimer.scheduleAtFixedRate(clusterConnector, epochTimeInMS* 500, epochTimeInMS * 1000 * 10);
    }

    /**
     * The process roughly is:
     * 1. Create a new thread for each tid
     * 2. Get remote cluster ID
     * 3. create a map of transaction=>clusterid=>status of prepare message
     * 4. once responses of prepareOperationObserver messages start coming back, update the transaction status.
     * 5. Once all responses are received call processor.prepareOperationObserver(tid)
     * This is done by the @{@link PrepareProcessor}
     *
     * @param tid
     * @param request
     */
    public void prepareAsync(final TransactionID tid, final Bzs.Transaction request) {
        LOG.info("Executing remote transaction PREPARE with tid: "+ tid.toString()+", Transaction request: "+request);
        runProcessor(new PrepareProcessor(tid, request, clusterID, replicaID, clusterConnector));
    }

    private void runProcessor(RemoteOpProcessor remoteProcessor) {
        remoteProcessor.setResponseObserver(processor);
        new Thread(remoteProcessor).start();
    }

    public void commitAsync(final TransactionID tid, final Bzs.Transaction request) {
        LOG.info("Executing remote transaction COMMIT with tid: "+ tid.toString()+", Transaction request: "+request);
        runProcessor(new CommitProcessor(clusterID, replicaID, tid, request, clusterConnector));
    }

    public void abortAsync(final TransactionID tid, final Bzs.Transaction request) {
        LOG.info("Executing remote transaction ABORT with tid: "+ tid.toString()+", Transaction request: "+request);
        runProcessor(new AbortProcessor(clusterID,replicaID,tid,request,clusterConnector));
    }

    public void setObserver(TransactionProcessor transactionProcessor) {
        this.processor = transactionProcessor;
    }
}