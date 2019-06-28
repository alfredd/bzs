package edu.ucsc.edgelab.db.bzs.cluster;

import edu.ucsc.edgelab.db.bzs.Bzs;

import java.util.Map;
import java.util.logging.Logger;

public class ClusterClient {

    private Map<Integer, ClusterServiceClient> clients;

    public static final Logger LOG = Logger.getLogger(ClusterClient.class.getName());

    public ClusterClient(Map<Integer, ClusterServiceClient> clients) {
        this.clients = clients;
    }

    public Bzs.ReadResponse read(Bzs.Read readOperation, Integer clusterID) {
        if (this.clients.containsKey(clusterID))
            return getClusterServiceClient(clusterID).read(readOperation);
        return null;
    }

    /**
     * This is a call to the server to commit the data.
     *
     * @param transaction
     * @param clusterID
     * @return
     */
    public Bzs.TransactionResponse commit(Bzs.Transaction transaction, Integer clusterID) {
        if (this.clients.containsKey(clusterID))
            return getClusterServiceClient(clusterID).commit(transaction);
        LOG.info("Cluster client for cluster ID: " + clusterID + ", not found");
        return null;
    }

    private ClusterServiceClient getClusterServiceClient(Integer clusterID) {
        return this.clients.get(clusterID);
    }

    /**
     * Call is exactly the same as commit but execution at server is different. This is a prepare message.
     *
     * @param transaction
     * @param clusterID
     * @return
     */
    public Bzs.TransactionResponse prepare(Bzs.Transaction transaction, Integer clusterID) {
        return getClusterServiceClient(clusterID).prepare(transaction);
    }

    public Bzs.TransactionResponse abort(Bzs.Transaction transaction, Integer clusterID) {
        return getClusterServiceClient(clusterID).abort(transaction);
    }

    public static enum DRWT_Operations {
        PREPARE_BATCH,
        COMMIT_BATCH,
        ABORT_BATCH
    }

    public Bzs.TransactionBatchResponse execute(DRWT_Operations operation, Bzs.TransactionBatch batch, Integer clusterID) {
        ClusterServiceClient clusterServiceClient = getClusterServiceClient(clusterID);
        if (clusterServiceClient != null) {
            switch (operation) {
                case COMMIT_BATCH:
                    return clusterServiceClient.commitAll(batch);
                case PREPARE_BATCH:
                    return clusterServiceClient.prepareAll(batch);
                case ABORT_BATCH:
                    return clusterServiceClient.abortAll(batch);
            }
        }
        return null;
    }

}
