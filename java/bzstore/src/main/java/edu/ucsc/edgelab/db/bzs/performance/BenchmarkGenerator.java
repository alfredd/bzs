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
import edu.ucsc.edgelab.db.bzs.txn.TxnUtils;

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

        Iterator<String> keys = storedData.keySet().iterator();
        int ops = (readOpCountPerTxn + writeOpCountPerTxn) / 2;
        for (int k = 0; k < storedData.size(); k++) {
            ConnectionLessTransaction t = new ConnectionLessTransaction();

            for (int j = 0; j < ops; j++) {
                BZStoreData storeData;
                String key;
                if (keys.hasNext()) {
                    key = keys.next();
                    storeData = storedData.get(key);
                } else {
                    break;
                }
                if (storeData.version > 0) {
                    t.setReadHistory(key, storeData.value, storeData.version, clusterID);
                    t.write(key, storeData.value + 1, clusterID);
                }
            }
            Bzs.Transaction txn = t.getTransaction();
            transactions.add(txn);
        }
        log.info("DEBUG: Generated L-RWTs:  " + transactions.size());
        return transactions;
    }

    public LinkedList<Bzs.Transaction> createMixedTransactions(int lrwtRatio, int drwtRatio, LinkedList<String> localClusterKeys,
                                                               LinkedList<String> remoteClusterKeys) {
        LinkedList<Bzs.Transaction> txnList = new LinkedList<>();

        return txnList;
    }

    public LinkedList<Bzs.Transaction> generate_DRWTransactions(LinkedList<String> localClusterKeys, LinkedList<String> remoteClusterKeys) {
        LinkedList<Bzs.Transaction> transactions = new LinkedList<>();
        loadKeysFromDB(localClusterKeys);

        Map<Integer, BZStoreClient> clientList = getBZStoreClientConnectionMap();

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

    private Map<Integer, BZStoreClient> getBZStoreClientConnectionMap() {
        Map<Integer, BZStoreClient> clientList = new LinkedHashMap<>();
        for (int i = 0; i < totalClusterCount; i++) {
            //if (!clusterID.equals(i)/* && ID.canRunBenchMarkTests()*/) {
            try {
                ServerInfo serverInfo = Configuration.getServerInfo(i, 0);
                BZStoreClient client = new BZStoreClient(serverInfo.host, serverInfo.port);
                clientList.put(i, client);
            } catch (Exception e) {
                log.log(Level.WARNING, "Exception occurred while creating client connection to cluster: " + i + ":  " + e.getLocalizedMessage(), e);
            }
            //}
        }
        return clientList;
    }

    private LinkedList<Bzs.Transaction> createDTxns(LinkedList<String> remoteClusterKeys, LinkedList<String> localClusterKeys, Map<Integer,
            BZStoreClient> clientList) {

        LinkedList<Bzs.Transaction> transactions = new LinkedList<>();

        Random rnd = new Random();
        while (true) {
            if (remoteClusterKeys.size() < 1 || localClusterKeys.size() < 1) {
                break;
            }
            Transaction drwt = new Transaction();
            int readOpCounter = 0;
            try {
                for (int i = 0; i < readOpCountPerTxn; i++) {
                    if (remoteClusterKeys.size() > 0 && localClusterKeys.size() > 0) {

                        String readKey = getKey(remoteClusterKeys, localClusterKeys, rnd);
                        Integer clusterID = hashmod(readKey, totalClusterCount);
                        drwt.setClient(clientList.get(clusterID));
                        drwt.read(readKey);
                        readOpCounter += 1;
                    } else {
                        break;
                    }
                }

/*                FOR ROTxns experiment.
                    for (int i = 0; i < writeOpCountPerTxn; i++) {
                    String writeKey = getKey(remoteClusterKeys, localClusterKeys, rnd);
                    drwt.write(writeKey, writeKey + i, hashmod(writeKey, totalClusterCount));
                }*/
            } catch (Exception e) {
                log.log(Level.WARNING, "Exception occurred while creating DRWT batch: " + e.getLocalizedMessage(), e);
                break;
            }
            if (readOpCounter > 0)
                transactions.addLast(drwt.getTransaction());
        }
        log.info("Total DRWT transactions: " + transactions.size());
        return transactions;
    }

    private String getKey(LinkedList<String> remoteClusterKeys, LinkedList<String> localClusterKeys, Random rnd) {
        String readKey = "";
        Collections.shuffle(remoteClusterKeys);
        Collections.shuffle(localClusterKeys);
        if (rnd.nextDouble() < 0.5) {
            readKey = remoteClusterKeys.removeFirst();

        } else {
            readKey = localClusterKeys.removeFirst();
        }
        return readKey;
    }

    public LinkedList<Bzs.Transaction> generate_Mixed_Transactions(LinkedList<String> localClusterKeys, LinkedList<String> remoteClusterKeys) {
        LinkedList<Bzs.Transaction> transactions = new LinkedList<>();
        Map<Integer, BZStoreClient> clientList = getBZStoreClientConnectionMap();
        int remoteKeyIndex = 0;
        for (int i = 0; i < localClusterKeys.size(); ) {
            boolean createDRWT = i % 10 == 0;

            Bzs.Transaction.Builder t = Bzs.Transaction.newBuilder();
            Transaction txn = new Transaction();
            if (createDRWT) {
                String localKey = localClusterKeys.get(i);
                BZStoreData dbEntry = BZDatabaseController.getlatest(localKey);
                txn.setReadHistory(localKey, dbEntry.value, dbEntry.version, hashmod(localKey, totalClusterCount));
                for (int j = 0; j < 4; j++) {
                    String remoteKey = remoteClusterKeys.get(remoteKeyIndex);
                    Integer remoteCID = hashmod(remoteKey, totalClusterCount);
                    txn.setClient(clientList.get(remoteCID));
                    txn.read(remoteKey);
                    remoteKeyIndex += 1;
                }
                i += 1;
            } else {

                for (int j = 0; j < 5; j++) {
                    String localKey = localClusterKeys.get(i + j);
                    BZStoreData dbEntry = BZDatabaseController.getlatest(localKey);
                    txn.setReadHistory(localKey, dbEntry.value, dbEntry.version, hashmod(localKey, totalClusterCount));
                }
                i += 5;
            }
            transactions.add(txn.getTransaction());
        }
        return transactions;
    }

    public void setTotalClusterCount(int totalClusters) {
        this.totalClusterCount = totalClusters;
    }
}
