package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.clientlib.TransactionManager;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class BenchmarkExecutor implements Runnable {

    private List<String> wordList = new LinkedList<>();
    private int transactionCount = 0;
    private int transactionsCompleted = 0;
    private int transactionsFailed = 0;
    private boolean started = false;
    private int previousCompleted = 0;
    private int currentCompleted = 0;


    private static final Logger LOGGER = Logger.getLogger(BenchmarkExecutor.class.getName());

    private final TransactionProcessor transactionProcessor;
    private final ReportBuilder reportBuilder;
    private int totalCount;
    private int processed;
    private int flushed = 0;

    public BenchmarkExecutor(Integer clusterID, TransactionProcessor transactionProcessor) throws IOException {
        this.transactionProcessor = transactionProcessor;

        BZStoreProperties properties = new BZStoreProperties();
        String dataFile = properties.getProperty(clusterID, BZStoreProperties.Configuration.data);

        String fileName = System.getProperty("user.dir") + "/"+dataFile;
        LOGGER.info("Data File path: " + fileName);
        File file = new File(fileName);
        Scanner scanner = new Scanner(file);
        Set<String> words = new LinkedHashSet<>();
        while (scanner.hasNext()) {
            String[] line = scanner.next().split(" ");

            for (String word : line)
                if (word != null)
                    words.add(word);
        }
        scanner.close();

        String[] fields = new String[]{"Epoch Number, ",
                "Total Transactions in Epoch, ",
                "Transactions Processed In Epoch(S), ",
                "Transactions Failed In Epoch(F), ",
                "Total Transaction Count, ",
                "Total Transactions Completed, ",
                "Total Transactions Failed, ",
                "Processing Time(ms), ",
                "Throughput(Tps), ",
                "Bytes processed (Bytes), ",
                "Throughput (Bps)\n"
        };

        reportBuilder = new ReportBuilder("Report_w_hash", fields);
        wordList.addAll(words);


        LOGGER.info("Total words read from file: " + wordList.size());

    }

    public Bzs.Transaction generateWriteSet() {
        TransactionManager transactionManager = new TransactionManager();
        Random random = new Random();
        int writeCount = 0;
        while (writeCount == 0)
            writeCount = random.nextInt(10);
        for (int i = 0; i < writeCount; i++) {
            int keyIndex = random.nextInt(wordList.size());
            int valueIndex = random.nextInt(wordList.size());
            transactionManager.write(wordList.get(keyIndex), wordList.get(valueIndex));
        }
        return transactionManager.getTransaction();
    }

    @Override
    public void run() {
        try {
            BZStoreProperties properties = new BZStoreProperties();
            String delay = properties.getProperty(BZStoreProperties.Configuration.delay_start);
            Integer delayMs = Integer.decode(delay);
            LOGGER.info("Benchmark tests will run after " + delay + "milliseconds");
            Thread.sleep(delayMs);
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
        started = true;
        int n = 12000;
        int m = 15;
        this.totalCount = n * m;
        this.processed = 0;
        sendNTransactions(n, m);

    }

    public void sendNTransactions(int n, int m) {
        while ((--n) >= 0) {
            sendWriteOnlyTransactions(m);
//            try {
//                Thread.sleep(20);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }

    public void sendWriteOnlyTransactions(int i) {

        while ((i--) > 0) {
            Bzs.Transaction writeTransaction = generateWriteSet();
            StreamObserver<Bzs.TransactionResponse> responseObserver = getTransactionResponseStreamObserver();
            transactionProcessor.processTransaction(writeTransaction, responseObserver);
            transactionCount += 1;
        }
    }

    public void sendReadOnlyTransactions(int i) {

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
        currentCompleted = transactionsProcessedInEpoch == 0 ? transactionsFailedInEpoch : transactionsProcessedInEpoch;
        processed += currentCompleted;
        if (started && currentCompleted != previousCompleted) {
            flushed = 0;
            LOGGER.info(String.format("Total: %d, Completed: %d, Error: %d", transactionCount, transactionsCompleted,
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
        }
    }

}
