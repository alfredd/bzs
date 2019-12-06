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
    private Integer readOpCountPerTxn;
    private Integer writeOpCountPerTxn;
    private int totalClusterCount;
    private static final Logger log = Logger.getLogger(BenchmarkGenerator.class.getName());
    private Map<String, BZStoreData> storedData = new HashMap<>();


    public BenchmarkGenerator(Integer readOpCount, Integer writeOpCount) {
        this.clusterID = ID.getClusterID();
        this.readOpCountPerTxn = readOpCount;
        this.writeOpCountPerTxn = writeOpCount;
    }

    private void loadKeysFromDB(List<String> clusterKeys) {
        for (String dbKey : clusterKeys) {
            Integer wordHash = hashmod(dbKey, totalClusterCount);
            if (wordHash == clusterID) {
                BZStoreData storedValue = BZDatabaseController.getlatest(dbKey);
                storedData.put(dbKey, storedValue);
//                log.info("DEBUG: Key : "+dbKey+", Stored value: " + storedValue);
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
        log.info("DEBUG: Generated L-RWTs:  " + transactions.size());
        return transactions;
    }

    // TODO: pass localOnly Keys and remoteOnly Keys to the function to generate transactions.
    public LinkedList<Bzs.Transaction> generate_DRWTransactions(LinkedList<String> localClusterKeys, LinkedList<String> remoteClusterKeys,
                                                                int writeOpCount) {
        LinkedList<Bzs.Transaction> transactions = new LinkedList<>();
        loadKeysFromDB(localClusterKeys);

        Map<Integer, BZStoreClient> clientList = new LinkedHashMap<>();
        for (int i = 0; i < totalClusterCount; i++) {
            //if (!clusterID.equals(i)/* && ID.canRunBenchMarkTests()*/) {
            try {
                ServerInfo serverInfo = Configuration.getServerInfo(i, 0);
                BZStoreClient client = new BZStoreClient(serverInfo.host, serverInfo.port);
                clientList.put(i, client);
            } catch (Exception e) {
                log.log(Level.WARNING, "Exception occurred while creating client connection to cluster: "+ i+ ":  " + e.getLocalizedMessage(), e);
            }
            //}
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
        transactions = createDTxns(remoteClusterKeySet, localClusterKeySet, clientList);
/*        int newWriteStartsAt = 0;

        for (String localKey : localClusterKeySet) {
            Transaction t = new Transaction();
            List<String> transactionKeys = new LinkedList<>();
            int writeOperationsCount = writeOpCount;
            boolean endLoop = false;
            while (writeOperationsCount > 0) {
                String remoteClusterKey;
                try {
                    remoteClusterKey = remoteClusterKeySet.removeFirst();
                } catch (Exception e) {
//                    log.log(Level.WARNING, "No more keys in remoteClusterKeySet");
                    endLoop = true;
                    break;
                }
                int remoteClusterID = hashmod(remoteClusterKey, totalClusterCount);
                BZStoreClient client = clientList.get(remoteClusterID);
                if (client != null) {
                    t.setClient(client);
                    BZStoreData readResponse = t.read(remoteClusterKey);
//                    log.info("DEBUG: read response: " + readResponse);
//                    if (!readResponse.value.equalsIgnoreCase("")) {
                    writeOperationsCount -= 1;
                    transactionKeys.add(remoteClusterKey);
//                    }
                }
            }

//            log.info("DEBUG: creating transactions from remoteClusterKey: "+transactionKeys);

            BZStoreData localData = storedData.get(localKey);
            if (localData != null && transactionKeys.size() > 0) {
                t.setReadHistory(localKey, localData.value, localData.version, clusterID);

                for (String remoteTransactionKey : transactionKeys) {
                    t.write(remoteTransactionKey, remoteTransactionKey + newWriteStartsAt, hashmod(remoteTransactionKey, totalClusterCount));
                    newWriteStartsAt += 1;
                }
                transactions.add(t.getTransaction());
            }
        }*/

        for (Map.Entry<Integer, BZStoreClient> client : clientList.entrySet()) {
            try {
                client.getValue().shutdown();
            } catch (InterruptedException e) {
                log.log(Level.WARNING, "Could not stop cluster client: " + client.getKey(), e);
            }
        }

        log.info("Number of transactions for testing D-RW Txns: " + transactions.size()/*+ ". Transactions: "+transactions*/);
        return transactions;
    }

    private LinkedList<Bzs.Transaction> createDTxns(LinkedList<String> remoteClusterKeys, LinkedList<String> localClusterKeys, Map<Integer,
            BZStoreClient> clientList) {

        LinkedList<Bzs.Transaction> transactions = new LinkedList<>();

        Random rnd = new Random();
        while (true) {
            if (remoteClusterKeys.size() <= 0 || localClusterKeys.size() <= 0) {
                break;
            }
            Transaction drwt = new Transaction();

            try {
                for (int i = 0; i < readOpCountPerTxn; i++) {
                    String readKey = getKey(remoteClusterKeys, localClusterKeys, rnd);
                    Integer clusterID = hashmod(readKey, totalClusterCount);
                    drwt.setClient(clientList.get(clusterID));
                    drwt.read(readKey);
                }

                for (int i = 0; i < writeOpCountPerTxn; i++) {
                    String writeKey = getKey(remoteClusterKeys, localClusterKeys, rnd);
                    drwt.write(writeKey, writeKey + i, clusterID);
                }
            } catch (Exception e) {
                log.log(Level.WARNING, "Exception occurred while creating DRWT batch: " + e.getLocalizedMessage(), e);
                break;
            }
            transactions.addLast(drwt.getTransaction());
        }
        log.info("Total DRWT transactions: " + transactions.size());
        return transactions;
    }

    private String getKey(LinkedList<String> remoteClusterKeys, LinkedList<String> localClusterKeys, Random rnd) {
        String readKey = "";
        int remoteSize = remoteClusterKeys.size();
        int localSize = localClusterKeys.size();
        if (rnd.nextDouble() < 0.5) {
            readKey = remoteClusterKeys.remove(rnd.nextInt(remoteSize));

        } else {
            readKey = localClusterKeys.remove(rnd.nextInt(localSize));
        }
        return readKey;
    }

    public void setTotalClusterCount(int totalClusters) {
        this.totalClusterCount = totalClusters;
    }
}
