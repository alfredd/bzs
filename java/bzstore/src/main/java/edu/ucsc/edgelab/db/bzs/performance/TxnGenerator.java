package edu.ucsc.edgelab.db.bzs.performance;

import edu.ucsc.edgelab.db.bzs.BZStoreClient;
import edu.ucsc.edgelab.db.bzs.Bzs;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TxnGenerator {
    private final LinkedList<String> remoteKeys;
    private final LinkedList<String> localKeys;
    private final Map<Integer, BZStoreClient> clusterClients;
    private final int counter;

    public TxnGenerator(final LinkedList<String> remoteClusterKeys, final LinkedList<String> localClusterKeys, final Map<Integer,
            BZStoreClient> clientList) {

        this.remoteKeys = remoteClusterKeys;
        this.localKeys = localClusterKeys;
        this.clusterClients = clientList;
        counter = 0;
    }

    public List<Bzs.Transaction> generateTxns(int count) {
        LinkedList<Bzs.Transaction> transactions = null;

        return  transactions;
    }



}
