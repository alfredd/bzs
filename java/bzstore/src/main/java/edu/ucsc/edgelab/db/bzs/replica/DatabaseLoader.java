package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.clientlib.ConnectionLessTransaction;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.performance.BatchMetrics;
import edu.ucsc.edgelab.db.bzs.performance.BatchMetricsManager;
import edu.ucsc.edgelab.db.bzs.performance.BenchmarkGenerator;
import edu.ucsc.edgelab.db.bzs.txn.TransactionProcessorINTF;
import edu.ucsc.edgelab.db.bzs.txn.TxnUtils;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatabaseLoader implements Runnable {

    private final Integer clusterID;
    private LinkedList<String> wordList = new LinkedList<>();
    private int transactionCount = 0;
    private int transactionsCompleted = 0;
    private int transactionsFailed = 0;
    private boolean started = false;
    private long latency = 0;
    private long minLatency = 0;
    private long maxLatency = 0;
    private int previousCompleted = 0;
    private int currentCompleted = 0;
    private int totalClusters = 0;
    private int batchCounter = 0;

    private static final Logger log = Logger.getLogger(DatabaseLoader.class.getName());

    private final TransactionProcessorINTF transactionProcessor;
    //    private final ReportBuilder reportBuilder;
    private int totalCount;
    private int processed;
    private int flushed = 0;
    private final LinkedList<String> allWords;
    private final LinkedList<String> remoteClusterKeys = new LinkedList<>();
    private boolean sendLocalOnly;
    private Integer[] rwRatio;

    public DatabaseLoader(TransactionProcessorINTF transactionProcessor) throws IOException {
        this.transactionProcessor = transactionProcessor;
        this.clusterID = ID.getClusterID();

        BZStoreProperties properties = new BZStoreProperties();
        this.totalClusters = Integer.parseInt(properties.getProperty(BZStoreProperties.Configuration.cluster_count));
        setupRWRatio(properties);


        String dataFile = "data.txt";
        String fileName = System.getProperty("user.dir") + "/" + dataFile;
        log.info("Data File path: " + fileName);
        File file = new File(fileName);
        Scanner scanner = new Scanner(file);
//        Set<String> words = new LinkedHashSet<>();
        allWords = new LinkedList<>();
        log.info(String.format("Loading keys from: %s. Total number of clusters: %d", fileName, totalClusters));
        while (scanner.hasNext()) {
            String[] line = scanner.next().split(" ");
            /*if (line != null)
                log.info(String.format("Line read from file. Number of words: %d", line.length));
            else
                log.log(Level.WARNING, "Could not read data from file.");*/

            for (String word : line) {

                allWords.add(word);
                if (totalClusters == 1) {
                    wordList.add(word);
                } else {
                    Integer cid = TxnUtils.hashmod(word, totalClusters);
                    if (cid == clusterID) {
                        wordList.add(word);
                    } else {
                        remoteClusterKeys.add(word);
                    }
                }
            }
        }
        scanner.close();
        Collections.shuffle(wordList);
        Collections.shuffle(remoteClusterKeys);


        log.info("Total words read from file: " + wordList.size());

    }

    private void setupRWRatio(BZStoreProperties properties) {
        String[] strRWRatio = properties.getProperty(BZStoreProperties.Configuration.dtxn_read_write_ratio).trim().split(":");
        if (strRWRatio == null || strRWRatio.length != 2) {
            strRWRatio = new String[]{"1", "5"};
        }
        rwRatio = new Integer[2];
        try {
            rwRatio[0] = Integer.decode(strRWRatio[0]);
            rwRatio[1] = Integer.decode(strRWRatio[1]);
        } catch (Exception e) {
            log.log(Level.WARNING, "Could not parse RW Ratio for distributed transactions from config.properties." +
                    "Setting default RW ratio.");
            rwRatio[0] = 1;
            rwRatio[1] = 5;
        }
        log.info(String.format("RW Operation Ratio set to %d:%d", rwRatio[0], rwRatio[1]));
    }

    public List<Bzs.Transaction> generateWriteSet(List<String> wordList) {
        List<Bzs.Transaction> writeOnlyTransactions = new LinkedList<>();
        for (String word : wordList) {

            ConnectionLessTransaction transactionManager = new ConnectionLessTransaction();
            transactionManager.write(word, word + word, ID.getClusterID());
            writeOnlyTransactions.add(transactionManager.getTransaction());
        }
        return writeOnlyTransactions;
    }

    @Override
    public void run() {
        int delayMs = 20000;
        try {
            BZStoreProperties properties = new BZStoreProperties();
            String delay = properties.getProperty(BZStoreProperties.Configuration.delay_start);
            delayMs = Integer.decode(delay);
            log.info("DB Loader will run after " + delay + "milliseconds");
            Thread.sleep(delayMs);
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
        this.sendLocalOnly = true;
        started = true;
        int txnCount = wordList.size();
        int maxOperations = 3;
        this.totalCount = txnCount;
        this.processed = 0;
        sendWriteOnlyTransactions(wordList);
//        txnCount = 100;
        log.info(String.format("Total transactions for W-ONLY LRWT = %d", totalCount));

//        log.info("Completed local transactions. Waiting for " + delayMs + "milliseconds before sending distributed transactions.");
//        log.info("Sending "+ txnCount+" distributed transactions for processing.");
        waitForTransactionCompletion(delayMs, txnCount, "write-only");

        try {
            log.info("Waiting for " + delayMs + "ms before generating LRW Txns.");
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logClientMetrics("W-Only");
        //Reset variables
        resetVariables();

        BatchMetricsManager batchMetricsManager = transactionProcessor.getBatchMetricsManager();
        for (Map.Entry<Integer, BatchMetrics> entryset : batchMetricsManager.getBatchMetrics().entrySet()) {
            int epochNumber = entryset.getKey();
            BatchMetrics batchMetrics = entryset.getValue();
            log.info(String.format("(Epoch, StartTime, EndTime, TxnCount)=%d, %d, %d, %d", epochNumber, batchMetrics.startTime,
                    batchMetrics.endTime, batchMetrics.txnStartedCount));
        }
        batchMetricsManager.getBatchMetrics().clear();

        log.info("GENERATING L-RWT.");
        BenchmarkGenerator benchmarkGenerator = new BenchmarkGenerator(rwRatio[0], rwRatio[1]);
        benchmarkGenerator.setTotalClusterCount(totalClusters);
        LinkedList<Bzs.Transaction> txns = benchmarkGenerator.generateAndPush_LRWTransactions(wordList);
        Collections.shuffle(txns);
        totalCount = txns.size();
        currentCompleted = 0;

        log.info(String.format("Total transactions for LRWT = %d", totalCount));
        for (Bzs.Transaction txn : txns) {
            transactionProcessor.processTransaction(txn, getTransactionResponseStreamObserver());
        }
        waitForTransactionCompletion(delayMs, txns.size(), "L-RW");
        logClientMetrics("LRWTxns");
        resetVariables();

        log.info("DRWT-Can be run? " + ID.canRunBenchMarkTests());
        if (ID.canRunBenchMarkTests()) {

            for (int i = 1; i < 2; i++) {
                log.info("GENERATING D-RWT ( 1R, " + i + "W op).");

                LinkedList<Bzs.Transaction> drwtxns = benchmarkGenerator.generate_DRWTransactions(wordList, remoteClusterKeys);
                Collections.shuffle(drwtxns);
                totalCount = drwtxns.size();
                currentCompleted = 0;
                log.info(String.format("Total transactions for DRWT = %d", totalCount));
                for (Bzs.Transaction t : drwtxns) {
                    transactionProcessor.processTransaction(t, getTransactionResponseStreamObserver());
                }
                waitForTransactionCompletion(delayMs, drwtxns.size(), "D-RW");

            }
        }

        int counter = 3;
        try {
            log.info("Waiting for all distributed Txn to complete.");
            while ((transactionsFailed + transactionsCompleted) <= (totalCount - 1) && counter >= 0) {
                Thread.sleep(60 * 1000);
                counter--;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logClientMetrics("DRWTxns");
//        resetVariables();

        log.info("END OF BENCHMARK RUN.");
    }

    private void logClientMetrics(String txnType) {
        log.info(String.format("Total %s Txns  (total latency, completed, failed, min latency, max latency) = (%d, %d, %d, %d, %d)",
                txnType, latency, transactionsCompleted, transactionsFailed, minLatency, maxLatency));
    }

    private void resetVariables() {
        latency = 0;
        transactionsCompleted = 0;
        transactionsFailed = 0;
        transactionCount = 0;
        minLatency = Long.MAX_VALUE;
        maxLatency = Long.MIN_VALUE;
    }

    private void waitForTransactionCompletion(int delayMs, int txnCount, String transactionType) {
        int endCounter = 5;
        while (endCounter >= 0 && currentCompleted < totalCount) {
            try {
                Thread.sleep(delayMs);
                if (currentCompleted < txnCount) {
                    endCounter -= 1;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info(String.format("Total " + transactionType + " transactions: %d, Completed count: %d", totalCount, currentCompleted));
        }
    }


    public void sendWriteOnlyTransactions(List<String> wordList) {

        List<Bzs.Transaction> writeOnlyTransactions = generateWriteSet(wordList);
        Collections.shuffle(writeOnlyTransactions);
        for (Bzs.Transaction writeTransaction : writeOnlyTransactions) {
            StreamObserver<Bzs.TransactionResponse> responseObserver = getTransactionResponseStreamObserver();
            transactionProcessor.processTransaction(writeTransaction, responseObserver);
            transactionCount += 1;
        }
    }

    public StreamObserver<Bzs.TransactionResponse> getTransactionResponseStreamObserver() {
        return new MyStreamObserver();
    }

    public static void main(String[] args) {
        String[] words = {"abcd"};
        for (String word : words)
            System.out.println(word);
    }

    private class MyStreamObserver implements StreamObserver<Bzs.TransactionResponse> {

        private long startTime;
        private long endTime;
        private int count = 0;

        public MyStreamObserver() {
        }

        @Override
        public void onNext(Bzs.TransactionResponse transactionResponse) {
            if (count == 0) {
                startTime = System.currentTimeMillis();
                count = 1;
            } else {
                if (transactionResponse.getStatus().equals(Bzs.TransactionStatus.ABORTED)) {
                    transactionsFailed += 1;
                } else {
                    transactionsCompleted += 1;
                }
                currentCompleted = transactionsCompleted + transactionsFailed;
                endTime = System.currentTimeMillis();
                long txnLatency = endTime - startTime;
                DatabaseLoader.this.latency += txnLatency;
                if (minLatency > txnLatency) {
                    minLatency = txnLatency;
                }
                if (maxLatency < txnLatency) {
                    maxLatency = txnLatency;
                }
            }

        }

        @Override
        public void onError(Throwable throwable) {
//                transactionsFailed += 1;
        }

        @Override
        public void onCompleted() {

        }
    }

}

