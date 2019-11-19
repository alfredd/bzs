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

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.ucsc.edgelab.db.bzs.txn.TxnUtils.hashmod;

public class BenchmarkGenerator {

    private final Integer clusterID;
    private int totalClusterCount;
    private static final Logger log = Logger.getLogger(BenchmarkGenerator.class.getName());
    private Map<String, BZStoreData> storedData = new HashMap<>();

    public BenchmarkGenerator() {
        this.clusterID = ID.getClusterID();
    }

    private void loadKeysFromDB(List<String> clusterKeys) {
        for (String dbKey : clusterKeys) {
            Integer wordHash = hashmod(dbKey, totalClusterCount);
            if (wordHash == clusterID) {
                BZStoreData storedValue = BZDatabaseController.getlatest(dbKey);
                storedData.put(dbKey, storedValue);
                log.info("DEBUG: Key : "+dbKey+", Stored value: " + storedValue);
            }
        }
    }

    public LinkedList<Bzs.Transaction> generateAndPush_LRWTransactions(List<String> clusterKeys) {
        LinkedList<Bzs.Transaction> transactions = new LinkedList<>();
        loadKeysFromDB(clusterKeys);
        int i = 1;
        for (Map.Entry<String, BZStoreData> entry : storedData.entrySet()) {
            BZStoreData storeData = entry.getValue();
            if (storeData.version > 0) {
                ConnectionLessTransaction t = new ConnectionLessTransaction();
                t.setReadHistory(entry.getKey(), storeData.value, storeData.version, clusterID);
                t.write(entry.getKey(), storeData.value + i, clusterID);
                Bzs.Transaction txn = t.getTransaction();
                transactions.add(txn);
                i++;
            }
        }
        log.info("DEBUG: Generated L-RWTs:  "+transactions);
        return transactions;
    }

    // TODO: pass localOnly Keys and remoteOnly Keys to the function to generate transactions.
    public LinkedList<Bzs.Transaction> generate_DRWTransactions(LinkedList<String> localClusterKeys, LinkedList<String> remoteClusterKeys, int writeOpCount) {
        LinkedList<Bzs.Transaction> transactions = new LinkedList<>();
        loadKeysFromDB(localClusterKeys);

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

        LinkedList<String> remoteClusterKeySet = new LinkedList<>();
        LinkedList<String> localClusterKeySet = new LinkedList<>();
        for (String remoteKey : remoteClusterKeys) {
            remoteClusterKeySet.add(remoteKey.trim());
        }
        for (String localKey : localClusterKeys) {
            localClusterKeySet.add(localKey.trim());
        }
        int newWriteStartsAt = new Random().nextInt(100);

        for (String localKey : localClusterKeySet) {
            Transaction t = new Transaction();
            List<String> transactionKeys = new LinkedList<>();
            int writeOperationsCount = writeOpCount;
            boolean endLoop = false;
            while (writeOperationsCount>0) {
                String remoteClusterKey;
                try {
                    remoteClusterKey = remoteClusterKeySet.removeFirst();
                } catch (Exception e) {
                    endLoop = true;
                    break;
                }
                int remoteClusterID = hashmod(remoteClusterKey, totalClusterCount);
                BZStoreClient client = clientList.get(remoteClusterID);
                if (client!=null) {
                    t.setClient(client);
                    BZStoreData readResponse = t.read(remoteClusterKey);
                    if (!readResponse.value.equalsIgnoreCase("")) {
                        writeOperationsCount-=1;
                        transactionKeys.add(remoteClusterKey);
                    }
                }
            }
            if (endLoop)
                break;

            BZStoreData localData = storedData.get(localKey);
            if (localData != null) {
                t.setReadHistory(localKey, localData.value, localData.version, clusterID);

                for(String remoteTransactionKey: transactionKeys) {
                    t.write(remoteTransactionKey, remoteTransactionKey+newWriteStartsAt, hashmod(remoteTransactionKey, totalClusterCount));
                    newWriteStartsAt+=1;
                }
            } else {
                continue;
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
