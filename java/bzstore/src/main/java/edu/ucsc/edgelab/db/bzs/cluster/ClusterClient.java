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

    private ClusterServiceClient getClusterServiceClient(Integer clusterID) {
        return this.clients.get(clusterID);
    }

    public enum DRWT_Operations {
        PREPARE_BATCH,
        COMMIT_BATCH,
        ABORT_BATCH,
        PREPARE,
        COMMIT,
        ABORT
    }

    public Bzs.TransactionResponse execute(DRWT_Operations operation, Bzs.Transaction batch, Integer clusterID) {
        ClusterServiceClient clusterServiceClient = getClusterServiceClient(clusterID);
        if (clusterServiceClient != null) {
            switch (operation) {
                case COMMIT:
                    return clusterServiceClient.commit(batch);
                case PREPARE:
                    return clusterServiceClient.prepare(batch);
                case ABORT:
                    return clusterServiceClient.abort(batch);
            }
        }
        return null;
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
