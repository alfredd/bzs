package edu.ucsc.edgelab.db.bzs.performance;

import edu.ucsc.edgelab.db.bzs.BZStoreClient;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.clientlib.ConnectionLessTransaction;
import edu.ucsc.edgelab.db.bzs.clientlib.Transaction;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.configuration.ServerInfo;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.replica.ID;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.ucsc.edgelab.db.bzs.replica.DatabaseLoader.hashmod;

public class BenchmarkGenerator {

    private final Integer clusterID;
    private int totalClusterCount;
    private static final Logger log = Logger.getLogger(BenchmarkGenerator.class.getName());
    private Map<String, BZStoreData> storedData = new HashMap<>();

    public BenchmarkGenerator() {
        this.clusterID = ID.getClusterID();
    }

    private void loadKeysFromFileForCluster(List<String> words) {
        for (String word : words) {
            Integer wordHash = hashmod(word, totalClusterCount);
            if (wordHash == clusterID) {
                BZStoreData storedValue = BZDatabaseController.getlatest(word);
                storedData.put(word, storedValue);
            }
        }
    }

    public LinkedList<Bzs.Transaction> generateAndPush_LRWTransactions(List<String> words) {
        LinkedList<Bzs.Transaction> transactions = new LinkedList<>();
        loadKeysFromFileForCluster(words);
        for (Map.Entry<String, BZStoreData> entry : storedData.entrySet()) {
            BZStoreData storeData = entry.getValue();
            if (storeData.version > 0) {
                ConnectionLessTransaction t = new ConnectionLessTransaction();
                t.setReadHistory(entry.getKey(), storeData.value, storeData.version, clusterID);
                t.write(entry.getKey(), storeData.value + storeData.value, clusterID);
                Bzs.Transaction txn = t.getTransaction();
                transactions.add(txn);
            }
        }
        return transactions;
    }

    // TODO: pass localOnly Keys and remoteOnly Keys to the function to generate transactions.
    public LinkedList<Bzs.Transaction> generate_DRWTransactions(List<String> localClusterKeys, List<String> remoteClusterKeys) {
        LinkedList<Bzs.Transaction> transactions = new LinkedList<>();
        loadKeysFromFileForCluster(localClusterKeys);

        Map<Integer, BZStoreClient> clientList = new LinkedHashMap<>();
        for (int i = 0; i < totalClusterCount; i++) {
            if (!clusterID.equals(i) && ID.canRunBenchMarkTests()) {
                try {
                    ServerInfo serverInfo = Configuration.getServerInfo(i, 0);
                    BZStoreClient client = new BZStoreClient(serverInfo.host, serverInfo.port);
                    clientList.put(i, client);
                } catch (Exception e) {
                    log.log(Level.WARNING, "Exception occurred while retrieving data from client. "+e.getLocalizedMessage(), e);
                }
            }
        }

        int remoteMaxKeyCount = remoteClusterKeys.size();
        int remoteIndex = 0;
        for (String localKey : localClusterKeys) {
            Transaction t = new Transaction();
            if (remoteIndex >= remoteMaxKeyCount) {
                break;
            } else {
                String remoteClusterKey = remoteClusterKeys.get(remoteIndex);
                int remoteClusterID = hashmod(remoteClusterKey, totalClusterCount);
                BZStoreClient client = clientList.get(remoteClusterID);
                if (client!=null) {
                    t.setClient(client);
                    t.read(remoteClusterKey);
                }
                remoteIndex += 1;
            }
            BZStoreData localData = storedData.get(localKey);
            if (localData != null) {
                t.setReadHistory(localKey, localData.value, localData.version, clusterID);
            }
            transactions.add(t.getTransaction());
        }

        log.info("Number of transactions for testing D-RW Txns: "+ transactions.size());
        return transactions;
    }

    public void setTotalClusterCount(int totalClusters) {
        this.totalClusterCount = totalClusters;
    }
}
