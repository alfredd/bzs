package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.clientlib.ConnectionLessTransaction;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
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
    private List<String> wordList = new LinkedList<>();
    private int transactionCount = 0;
    private int transactionsCompleted = 0;
    private int transactionsFailed = 0;
    private boolean started = false;
    private int previousCompleted = 0;
    private int currentCompleted = 0;
    private int totalClusters = 0;

    private static final Logger log = Logger.getLogger(DatabaseLoader.class.getName());

    private final TransactionProcessorINTF transactionProcessor;
    //    private final ReportBuilder reportBuilder;
    private int totalCount;
    private int processed;
    private int flushed = 0;
    private final List<String> allWords;
    private final List<String> remoteClusterKeys = new LinkedList<>();
    private boolean sendLocalOnly;
    private List<String> writeOnlyWordList = new LinkedList<>();

    public DatabaseLoader(TransactionProcessorINTF transactionProcessor) throws IOException {
        this.transactionProcessor = transactionProcessor;
        this.clusterID = ID.getClusterID();

        BZStoreProperties properties = new BZStoreProperties();
        this.totalClusters = Integer.parseInt(properties.getProperty(BZStoreProperties.Configuration.cluster_count));
        //String dataFile = properties.getProperty(clusterID, BZStoreProperties.Configuration.data);
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
            if (line != null)
                log.info(String.format("Line read from file. Number of words: %d", line.length));
            else
                log.log(Level.WARNING, "Could not read data from file.");

            for (String word : line) {

                allWords.add(word);
                if (totalClusters==1) {
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


        log.info("Total words read from file: " + wordList.size());

    }

    public Bzs.Transaction generateWriteSet(Integer operationCount) {
        ConnectionLessTransaction transactionManager = new ConnectionLessTransaction();
        Random random = new Random();
        List<String> wordListForGeneratingWriteSet = this.wordList;
        if (!sendLocalOnly) {
            wordListForGeneratingWriteSet = allWords;
        }
        int writeCount = 0;
        while (writeCount == 0) {
            writeCount = random.nextInt(operationCount);
        }
        for (int i = 0; i < writeCount; i++) {
            int keyIndex = random.nextInt(wordListForGeneratingWriteSet.size());
            int valueIndex = random.nextInt(wordListForGeneratingWriteSet.size());
            int cid = clusterID;
            String key = wordListForGeneratingWriteSet.get(keyIndex);
            this.writeOnlyWordList.add(key);
            if (!sendLocalOnly) {
                cid = TxnUtils.hashmod(key, totalClusters);
            }
            transactionManager.write(key, wordListForGeneratingWriteSet.get(valueIndex), cid);
        }
        return transactionManager.getTransaction();
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
        int txnCount = 6000;
        int maxOperations = 8;
        this.totalCount = txnCount;
        this.processed = 0;
        sendTransactions(txnCount, maxOperations);
        txnCount = 100;
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

        log.info("GENERATING L-RWT.");
        BenchmarkGenerator benchmarkGenerator = new BenchmarkGenerator();
        benchmarkGenerator.setTotalClusterCount(totalClusters);
        LinkedList<Bzs.Transaction> txns = benchmarkGenerator.generateAndPush_LRWTransactions(writeOnlyWordList);
        totalCount = txns.size();
        currentCompleted = 0;

        log.info(String.format("Total transactions for LRWT = %d", totalCount));
        for (Bzs.Transaction txn : txns) {
            transactionProcessor.processTransaction(txn, getTransactionResponseStreamObserver());
        }
        waitForTransactionCompletion(delayMs, txns.size(), "L-RW");
        log.info("DRWT-Can be run? " + ID.canRunBenchMarkTests());
        if (ID.canRunBenchMarkTests()) {

            log.info("GENERATING D-RWT.");

            LinkedList<Bzs.Transaction> drwtxns = benchmarkGenerator.generate_DRWTransactions(wordList, remoteClusterKeys);
            totalCount = drwtxns.size();
            currentCompleted = 0;
            log.info(String.format("Total transactions for DRWT = %d", totalCount));
            for (Bzs.Transaction t : drwtxns) {
                transactionProcessor.processTransaction(t, getTransactionResponseStreamObserver());
            }
            waitForTransactionCompletion(delayMs, txns.size(), "D-RW");
        }
        log.info("END OF BENCHMARK RUN.");

    }

    private void waitForTransactionCompletion(int delayMs, int txnCount, String transactionType) {
        int end = 0;
        while (end < 1) {
            try {
                Thread.sleep(delayMs);
                if (currentCompleted >= txnCount) {
                    end += 1;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info(String.format("Total " + transactionType + " transactions: %d, Completed count: %d", totalCount, currentCompleted));
        }
    }

    public void sendTransactions(int n, int m) {
        while ((--n) >= 0) {
            sendWriteOnlyTransactions(m);
        }
    }

    public void sendWriteOnlyTransactions(int totalOperations) {

        Bzs.Transaction writeTransaction = generateWriteSet(totalOperations);
        StreamObserver<Bzs.TransactionResponse> responseObserver = getTransactionResponseStreamObserver();
        transactionProcessor.processTransaction(writeTransaction, responseObserver);
        transactionCount += 1;
    }

    public StreamObserver<Bzs.TransactionResponse> getTransactionResponseStreamObserver() {
        return new StreamObserver<Bzs.TransactionResponse>() {
            @Override
            public void onNext(Bzs.TransactionResponse transactionResponse) {
                if (transactionResponse.getStatus().equals(Bzs.TransactionStatus.ABORTED)) {
                    transactionsFailed += 1;
                } else {
                    transactionsCompleted += 1;
                }
                currentCompleted = transactionsCompleted + transactionsFailed;
            }

            @Override
            public void onError(Throwable throwable) {
//                transactionsFailed += 1;
            }

            @Override
            public void onCompleted() {

            }
        };
    }

    public void logTransactionDetails(int epochNumber, int epochTransactionCount, int transactionsProcessedInEpoch,
                                      int transactionsFailedInEpoch, long epochProcessingStartTime,
                                      long epochProcessingEndTime, int bytesProcessedInEpoch) {
 /*       currentCompleted = transactionsProcessedInEpoch == 0 ? transactionsFailedInEpoch : transactionsProcessedInEpoch;
        processed += currentCompleted;
        if (started && currentCompleted != previousCompleted) {
            flushed = 0;
            log.info(String.format("Total: %d, Completed: %d, Error: %d", transactionCount, transactionsCompleted,
                    transactionsFailed));
            long latency = epochProcessingEndTime - epochProcessingStartTime;
            double throughputTps = latency == 0 ? 0 : (double) transactionsProcessedInEpoch * 1000 / (latency);
            double throughputBps = latency == 0 ? 0 : (double) bytesProcessedInEpoch * 1000 / (latency);
            String report = String.format("%d, %d, %d, %d, %d, %d, %d, %d, %f, %d, %f\n",
                    epochNumber,
                    epochTransactionCount,
                    transactionsProcessedInEpoch,
                    transactionsFailedInEpoch,
                    transactionCount,
                    transactionsCompleted,
                    transactionsFailed,
                    latency,
                    throughputTps,
                    bytesProcessedInEpoch,
                    throughputBps
            );
            if (reportBuilder != null) {
                reportBuilder.writeLine(report);
            }
        } else {
            if (flushed == 0) {
                flushed = 1;
                reportBuilder.flush();
            }
        }*/
    }

    public static void main(String[] args) {
        String[] words={"abcd"};
        for (String word: words)
            System.out.println(word);
    }

}
