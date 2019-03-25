package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.clientlib.TransactionManager;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BenchmarkExecutor implements Runnable {

    private List<String> wordList = new LinkedList<>();
    private int transactionCount = 0;
    private int transactionsCompleted = 0;
    private int transactionsFailed = 0;
    private boolean started = false;


    private static final Logger LOGGER = Logger.getLogger(BenchmarkExecutor.class.getName());

    private final TransactionProcessor transactionProcessor;
    private FileWriter writer = null;

    public BenchmarkExecutor(TransactionProcessor transactionProcessor) throws FileNotFoundException {
        this.transactionProcessor = transactionProcessor;
        String fileName = System.getProperty("user.dir") + "//src/main/resources/ulysses.txt";
        String reportFileName = System.getProperty("user.dir") + "/Report_" + getDateString() + ".csv";
        LOGGER.info("Filename: " + fileName);
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
        try {
            writer = new FileWriter(new File(reportFileName));

            writer.write("Epoch Number, " +
                    "Total Transactions in Epoch, " +
                    "Transactions Processed In Epoch(S), " +
                    "Transactions Failed In Epoch(F), " +
                    "Total Transaction Count, " +
                    "Total Transactions Completed, " +
                    "Total Transactions Failed, " +
                    "Processing Time(ms)\n");
        } catch (IOException e) {
            LOGGER.info("Error occurred while creating report file: " + reportFileName);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down report writer.");
            try {
                writer.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING,"Failed to close report file writer.",e);
            }
        }));
        wordList.addAll(words);


        LOGGER.info("Total words read from file: " + wordList.size());

    }

    public Bzs.Transaction generateWriteSet() {
        TransactionManager transactionManager = new TransactionManager();
        Random random = new Random();
        int writeCount=0;
        while (writeCount==0)
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
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        started = true;
        sendNTransactions(100);

    }

    public void sendNTransactions(int n) {
        while ((--n)>=0) {
            sendTransactions(10);
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void sendTransactions(int i) {

        while ((i--) > 0) {
            Bzs.Transaction writeTransaction = generateWriteSet();
            StreamObserver<Bzs.TransactionResponse> responseObserver = getTransactionResponseStreamObserver();
            transactionProcessor.processTransaction(writeTransaction, responseObserver);
            transactionCount += 1;
        }
    }

    public StreamObserver<Bzs.TransactionResponse> getTransactionResponseStreamObserver() {
        return new StreamObserver<Bzs.TransactionResponse>() {
            @Override
            public void onNext(Bzs.TransactionResponse transactionResponse) {
                if (transactionResponse.getStatus().equals(Bzs.TransactionStatus.ABORTED)) {
                    transactionsFailed+=1;
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
                                      long epochProcessingEndTime) {
        if (started) {
            LOGGER.info(String.format("Total: %d, Completed: %d, Error: %d", transactionCount, transactionsCompleted,
                    transactionsFailed));
            String report = String.format("%d, %d, %d, %d, %d, %d, %d, %d \n",
                    epochNumber,
                    epochTransactionCount,
                    transactionsProcessedInEpoch,
                    transactionsFailedInEpoch,
                    transactionCount,
                    transactionsCompleted,
                    transactionsFailed,
                    epochProcessingEndTime - epochProcessingStartTime);
            if (writer != null) {
                try {
                    writer.write(report);
                } catch (IOException e) {
                    LOGGER.warning("Exception occurred while writing report: " + report);
                }
            }
        }
    }

    public static void main(String args[]) throws FileNotFoundException {
        BenchmarkExecutor benchmarkExecutor = new BenchmarkExecutor(null);
        String format = getDateString();
        System.out.println(format);

    }

    public static String getDateString() {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd.HH.mm.ss");
        return sdf.format(date);
    }
}
