package edu.ucsc.edgelab.db.bzs.cluster;

import edu.ucsc.edgelab.db.bzs.BZStoreClient;
import edu.ucsc.edgelab.db.bzs.Bzs;

import java.util.Map;

public class ClusterClient {

    private Map<Integer, ClusterServiceClient> clients;

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
     * @param transaction
     * @param clusterID
     * @return
     */
    public Bzs.TransactionResponse commit(Bzs.Transaction transaction, Integer clusterID) {
        if (this.clients.containsKey(clusterID))
            return getClusterServiceClient(clusterID).commit(transaction);
        return null;
    }

    private ClusterServiceClient getClusterServiceClient(Integer clusterID) {
        return this.clients.get(clusterID);
    }

    /**
     * Call is exactly the same as commit but execution at server is different. This is a prepare message.
     * @param transaction
     * @param clusterID
     * @return
     */
    public Bzs.TransactionResponse prepare(Bzs.Transaction transaction, Integer clusterID) {
        return getClusterServiceClient(clusterID).prepare(transaction);
    }

    public Bzs.TransactionResponse abort (Bzs.Transaction transaction, Integer clusterID) {
        return getClusterServiceClient(clusterID).abort(transaction);
    }

}
