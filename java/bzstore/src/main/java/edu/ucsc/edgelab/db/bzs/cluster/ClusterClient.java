package edu.ucsc.edgelab.db.bzs.cluster;

import edu.ucsc.edgelab.db.bzs.BZStoreClient;
import edu.ucsc.edgelab.db.bzs.Bzs;

import java.util.Map;

public class ClusterClient {

    private Map<Integer, BZStoreClient> clients;

    public ClusterClient(Map<Integer, BZStoreClient> clients) {
        this.clients = clients;
    }

    public Bzs.ReadResponse read(Bzs.Read readOperation, Integer clusterID) {
        if (this.clients.containsKey(clusterID))
            return this.clients.get(clusterID).read(readOperation);
        return null;
    }

    public Bzs.TransactionResponse commit(Bzs.Transaction transaction, Integer clusterID) {
        if (this.clients.containsKey(clusterID))
            return this.clients.get(clusterID).commit(transaction);
        return null;
    }
}
