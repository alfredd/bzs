package edu.ucsc.edgelab.db.bzs.clientlib;

import edu.ucsc.edgelab.db.bzs.BZStoreClient;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.exceptions.CommitAbortedException;
import edu.ucsc.edgelab.db.bzs.txn.TxnUtils;
import edu.ucsc.edgelab.db.bzs.txnproof.DependencyValidator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Transaction extends ConnectionLessTransaction implements TransactionInterface {

    private BZStoreClient client;

    public static final Logger LOGGER = Logger.getLogger(Transaction.class.getName());


    public Transaction() {
        super();
    }

    public Transaction(String host, int port) {
        this();
        setClient(host, port);
    }

    public void setClient(String host, int port) {
        this.client = new BZStoreClient(host, port);
    }

    public void setClient(BZStoreClient client) {
        this.client = client;
    }

    @Override
    public BZStoreData read(String key) {
        long startTime = System.currentTimeMillis();
        Bzs.Read read = Bzs.Read.newBuilder().setKey(key).build();
        return getBzStoreDataFromCluster(startTime, read);
    }

    @Override
    public void commit() throws CommitAbortedException {
        long startTime = System.currentTimeMillis();
        LOGGER.info("Committing Transaction: "+ getTransaction().toString());
        Bzs.TransactionResponse response = client.commit(getTransaction());
        long duration = System.currentTimeMillis() - startTime;
        LOGGER.info("Transaction processed in "+duration+" msecs");
        LOGGER.info("Transaction Response: "+response);
        if(response.getStatus().equals(Bzs.TransactionStatus.ABORTED)) {
            LOGGER.info("Transaction was aborted.");
            throw new CommitAbortedException("Transaction was aborted"+response.toString());
        } else {
            if (response.getStatus().equals(Bzs.TransactionStatus.COMMITTED)) {
                LOGGER.info("Transaction committed. Transaction Response: "+response.toString());
            }
        }
    }

    public void close() {
        if (this.client!=null) {
            try {
                this.client.shutdown();
            } catch (InterruptedException e) {
                LOGGER.log(Level.WARNING,
                        "Exception occurred while closing client connection: "+e.getLocalizedMessage(),
                        e);
            }
        }
    }

    public BZStoreData read(String key, int clusterId) {
        long startTime = System.currentTimeMillis();
        Bzs.Read read = Bzs.Read.newBuilder().setKey(key).setClusterID(clusterId).build();
        return getBzStoreDataFromCluster(startTime, read);
    }

    public BZStoreData read (Bzs.Read read) {
        Bzs.ReadResponse response = client.read(read);
        BZStoreData data = new BZStoreData();
        data.value = response.getValue();
//        data.digest = response.getResponseDigest();
        data.version = response.getVersion();
        return data;
    }

    public BZStoreData getBzStoreDataFromCluster(long startTime, Bzs.Read read) {
        Bzs.ReadResponse response = client.read(read);
        BZStoreData data = new BZStoreData();
        String responseKey = response.getReadOperation().getKey();
        data.value = response.getValue();
//        data.digest = response.getResponseDigest();
        data.version = response.getVersion();
        setReadHistory(responseKey, data.value, data.version, response.getReadOperation().getClusterID());
//        long duration = System.currentTimeMillis() - startTime;

//        LOGGER.info("Read operation processed in " + duration + " msecs");
        return data;
    }

    public Map<String, String> readOnly(Map<Integer, List<Bzs.Read>> clusterKeyMap, HashMap<Integer, BZStoreClient> clientHashMap) {

        for (Map.Entry<Integer, List<Bzs.Read>> entry: clusterKeyMap.entrySet()) {
            setClient(clientHashMap.get(entry.getKey()));
            Bzs.ROTransaction roTransaction = Bzs.ROTransaction.newBuilder().addAllReadOperations(entry.getValue()).setClusterID(entry.getKey()).build();
            Bzs.ROTransactionResponse rotResponses = client.readOnly(roTransaction);
        }
        return null;
    }
}
