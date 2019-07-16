package edu.ucsc.edgelab.db.bzs.performance;

import edu.ucsc.edgelab.db.bzs.clientlib.ConnectionLessTransaction;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.txn.TxnProcessor;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Logger;

import static edu.ucsc.edgelab.db.bzs.replica.DatabaseLoader.hashmod;

public class BenchmarkGenerator {

    private int totalClusters;
    private static final Logger log = Logger.getLogger(BenchmarkGenerator.class.getName());
    private Set<String> words;

    public BenchmarkGenerator() {
        words = new LinkedHashSet<>();
    }

    private void loadKeysFromFileForCluster(int clusterID) throws IOException {
        BZStoreProperties properties = new BZStoreProperties();
        this.totalClusters = Integer.parseInt(properties.getProperty(BZStoreProperties.Configuration.cluster_count));
        File file = getDataKeysFile();
        Scanner scanner = new Scanner(file);
        LinkedList<Object> allWords = new LinkedList<>();
        while (scanner.hasNext()) {
            String[] line = scanner.next().split(" ");

            for (String word : line) {
                allWords.add(word);
                Integer cid = hashmod(word, totalClusters);
                if (cid == clusterID) {
                    words.add(word);
                }
            }
        }
        scanner.close();
    }

    private File getDataKeysFile() {
        String dataFile = "data.txt";
        String fileName = System.getProperty("user.dir") + "/" + dataFile;
        log.info("Data File path: " + fileName);
        return new File(fileName);
    }


    public void generateAndPushRWTransactions(TxnProcessor txnProcessor) {
        LinkedList<ConnectionLessTransaction> transactions = new LinkedList<>();



    }
}
