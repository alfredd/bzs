package edu.ucsc.edgelab;

import edu.ucsc.edgelab.db.bzs.BZStoreClient;
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
        BZStoreClient CurrClient = clientHashMap.get(0);
        transaction.setClient(CurrClient);
        LOGGER.info("Executing read on cluster: "+0);

        return transaction.read(key, clusterId);
    }

    public void write(String key, String value) {
        int clusterId = hashmod(key, total_clusters);
        transaction.setClient(clientHashMap.get(0));
        LOGGER.info("Executing write on cluster: "+0);
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
        dclient.createNewTransactions();
        String key = "Azerbaijan";
        BZStoreData data = dclient.read(key);
        LOGGER.info("Data from db: "+data);
        String key2 = "Minnesota";
        data = dclient.read(key2);
        LOGGER.info("Data from db: "+data);
        dclient.write(key, "Random Value 6");
        dclient.commit();
    }

    public static Integer hashmod(String key, int totalCluster) {
        return Math.abs(key.hashCode()) % totalCluster;
    }

}
