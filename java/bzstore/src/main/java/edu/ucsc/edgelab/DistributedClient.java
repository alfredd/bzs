package edu.ucsc.edgelab;

import edu.ucsc.edgelab.db.bzs.BZStoreClient;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.clientlib.Transaction;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.exceptions.CommitAbortedException;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DistributedClient {
    HashMap<Integer, BZStoreClient> clientHashMap = new HashMap<>();
    BZStoreProperties properties;
    private static final Logger LOGGER = Logger.getLogger(DistributedClient.class.getName());
    int total_clusters;
    Transaction transaction;

    DistributedClient() {
        try {
            properties = new BZStoreProperties();
        } catch (IOException e) {
            LOGGER.log(Level.INFO, e.getMessage());

        }

        total_clusters = Integer.parseInt(properties.getProperty(BZStoreProperties.Configuration.cluster_count));
        for (int i = 0; i < total_clusters; i++) {
            int leader_id = Integer.parseInt(properties.getProperty(i, BZStoreProperties.Configuration.leader));
            String leader_host = properties.getProperty(i, leader_id, BZStoreProperties.Configuration.host);
            int leader_port = Integer.parseInt(properties.getProperty(i, leader_id,
                    BZStoreProperties.Configuration.port));
            clientHashMap.put(i, new BZStoreClient(leader_host, leader_port));
        }

    }

    public void createNewTransactions() {
        transaction = new Transaction();
    }

    public BZStoreData read(String key) {
        int clusterId = hashmod(key, total_clusters);
        BZStoreClient CurrClient = clientHashMap.get(clusterId);
        transaction.setClient(CurrClient);
        LOGGER.info("Executing read on cluster: " + clusterId);

        return transaction.read(key, clusterId);
    }

    public void write(String key, String value) {
        int clusterId = hashmod(key, total_clusters);
        transaction.setClient(clientHashMap.get(clusterId));
        LOGGER.info("Executing write on cluster: " + clusterId);
        transaction.write(key, value, clusterId);
    }

    public void commit() {
        try {
            transaction.commit();
        } catch (CommitAbortedException e) {
            LOGGER.log(Level.INFO, e.getMessage());
        }
    }

    public static void main(String args[]) {
        DistributedClient dclient = new DistributedClient();
        String dataFile = "data.txt";
        String fileName = System.getProperty("user.dir") + "/" + dataFile;
        File file = new File(fileName);
        ArrayList<String> words = new ArrayList();
        try {
            Scanner scanner = new Scanner(file);
            while (scanner.hasNext()) {
                String[] line = scanner.next().split(" ");
                for (String word : line)
                    if (word != null) {
                        words.add(word);
                    }
            }
        } catch (Exception e) {
            LOGGER.log(Level.INFO, e.getMessage());
        }
        String key = "Niger";
        String key2 = "Tajikistan";
        dclient.createNewTransactions();
        dclient.write(key, "98");
        dclient.commit();

        dclient.createNewTransactions();
        dclient.write(key2, "44");
        dclient.commit();

        dclient.createNewTransactions();

        BZStoreData data = null;
        data = dclient.read(key);
        LOGGER.info("Data from db: " + data);

        data = dclient.read(key2);
        LOGGER.info("Data from db: " + data);

        dclient.write(key, "2");
        dclient.write(key2, "3");

        Bzs.Transaction t = dclient.transaction.getTransaction();
        long startTime = System.currentTimeMillis();

        dclient.commit();
        System.out.println("Commit processed in "+(System.currentTimeMillis()-startTime)+"ms");

//        dclient.createNewTransactions();
//        key="Zambia";
//        key2="Palestine";
//        data = dclient.read(key);
//        data = dclient.read(key2);
//        dclient.write(key, "Random Value 58");
//        dclient.write(key2, "Random Value 77");
////        Bzs.Transaction t = dclient.transaction.getTransaction();
//        startTime = System.currentTimeMillis();
//        dclient.commit();
//        System.out.println("Commit processed in "+(System.currentTimeMillis()-startTime)+"ms");


    }

    public static Integer hashmod(String key, int totalCluster) {
        return Math.abs(key.hashCode()) % totalCluster;
    }

}
