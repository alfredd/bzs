package edu.ucsc.edgelab;

import edu.ucsc.edgelab.db.bzs.BZStoreClient;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.clientlib.Transaction;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.exceptions.CommitAbortedException;
import edu.ucsc.edgelab.db.bzs.exceptions.ValidityException;
import edu.ucsc.edgelab.db.bzs.txn.TxnUtils;
import edu.ucsc.edgelab.db.bzs.txnproof.DependencyValidator;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DistributedClient {
    private HashMap<Integer, BZStoreClient> clientHashMap = new HashMap<>();
    private BZStoreProperties properties;
    private static final Logger LOGGER = Logger.getLogger(DistributedClient.class.getName());
    private int total_clusters;
    private Transaction transaction;
    private DependencyValidator validator;

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
        validator = new DependencyValidator();
    }

    public void createNewTransactions() {
        transaction = new Transaction();
    }

    public BZStoreData read(String key) {
        int clusterId = TxnUtils.hashmod(key, total_clusters);
        BZStoreClient CurrClient = clientHashMap.get(clusterId);
        transaction.setClient(CurrClient);
        LOGGER.info("Executing read on cluster: " + clusterId);

        return transaction.read(key, clusterId);
    }

    public void write(String key, String value) {
        int clusterId = TxnUtils.hashmod(key, total_clusters);
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

    public Bzs.ROTransactionResponse roTransaction(String[] keys) {
        Bzs.ROTransaction.Builder roTBuilder = Bzs.ROTransaction.newBuilder();
        Map<Integer, List<Bzs.Read>> clusterKeyMap = new HashMap<>();
        for (String key: keys) {
            Integer clusterID = TxnUtils.hashmod(key, Configuration.clusterCount());
            if (!clusterKeyMap.containsKey(clusterID)) {
                clusterKeyMap.put(clusterID, new LinkedList<>());
            }
            clusterKeyMap.get(clusterID).add(Bzs.Read.newBuilder().setKey(key).build());

//            Bzs.Read readOp = Bzs.Read.newBuilder().setKey(key).build();
//            roTBuilder = roTBuilder.addReadOperations(readOp);
        }
        Map<Bzs.ROTransaction, Bzs.ROTransactionResponse> roTransactionResponseMap = new LinkedHashMap<>();
        List<Bzs.ReadResponse> readResponses = new LinkedList<>();
        for (Map.Entry<Integer, List<Bzs.Read>> entry: clusterKeyMap.entrySet()) {
            Bzs.ROTransaction roTransaction = Bzs.ROTransaction.newBuilder().addAllReadOperations(entry.getValue()).setClusterID(entry.getKey()).build();
            transaction.setClient(clientHashMap.get(entry.getKey()));
            Bzs.ROTransactionResponse response = transaction.readOnly(roTransaction);
            readResponses.addAll(response.getReadResponsesList());
            roTransactionResponseMap.put(roTransaction, response);
        }

        // TODO: Validate Response object.
        try {
            int valid = validator.validate(readResponses);
        } catch (ValidityException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String args[]) throws InterruptedException {
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
        String key2 = "Ohio";
        String key = "Indonesia";
        long startTime;
        long duration;

//        dclient.createNewTransactions();
//        System.out.println(dclient.read(key2));
//        System.out.println(dclient.read(key));


/*


        dclient.createNewTransactions();
        dclient.write(key2, "aa");

        startTime = System.currentTimeMillis();
        dclient.commit();
        duration = System.currentTimeMillis() - startTime;
        System.out.println("Commit processed in "+ duration +"ms");






        dclient.createNewTransactions();
        dclient.write(key, "bb");
        startTime = System.currentTimeMillis();
        dclient.commit();
        duration = System.currentTimeMillis() - startTime;
        System.out.println("Commit processed in "+ duration +"ms");






        Thread.sleep(1000);

*/


        dclient.createNewTransactions();

        BZStoreData data = null;
        data = dclient.read(key);
        LOGGER.info("Data from db: " + data);

        data = dclient.read(key2);
        LOGGER.info("Data from db: " + data);

        dclient.write(key,  "xyz");
        dclient.write(key2, "abc");

        Bzs.Transaction t = dclient.transaction.getTransaction();

        Thread.sleep(5000);

        LOGGER.info("Committing transaction.");

        startTime = System.currentTimeMillis();
        dclient.commit();
        duration = System.currentTimeMillis() - startTime;
        System.err.println("Commit processed in "+ duration +"ms");




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

}
