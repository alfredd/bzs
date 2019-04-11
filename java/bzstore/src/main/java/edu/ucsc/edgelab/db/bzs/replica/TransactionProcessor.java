package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TransactionProcessor {

    private Integer clusterID;
    private Integer maxBatchSize;
    private Serializer serializer;
    private int sequenceNumber;
    private int epochNumber;
    private ResponseHandlerRegistry responseHandlerRegistry;
    private LocalDataVerifier localDataVerifier;

    private static final Logger LOGGER = Logger.getLogger(TransactionProcessor.class.getName());
    private Integer replicaID;
    private BenchmarkExecutor benchmarkExecutor;
    private BFTClient bftClient = null;

    public TransactionProcessor(Integer id, Integer clusterId) {
        this.replicaID = id;
        this.clusterID = clusterId;
        localDataVerifier = new LocalDataVerifier(clusterID);
        serializer = new Serializer();
        sequenceNumber = 0;
        epochNumber = 0;
        responseHandlerRegistry = new ResponseHandlerRegistry();
    }

    private void initMaxBatchSize() {
        try {
            BZStoreProperties properties = new BZStoreProperties();
            this.maxBatchSize =
                    Integer.decode(properties.getProperty(BZStoreProperties.Configuration.epoch_batch_size));
        } catch (Exception e) {
            LOGGER.log(Level.WARNING,
                    "Exception occurred when getting max batch size from config files: " + e.getLocalizedMessage(), e);
            maxBatchSize = 2000;
        }
        LOGGER.info("Maximum BatchSize is set to " + maxBatchSize);
    }

    public void initTransactionProcessor() {
        initMaxBatchSize();
        startBftClient();
        EpochManager epochManager = new EpochManager(this);
        epochManager.startEpochMaintenance();
        initLocalDatabase();
    }

    public void initLocalDatabase() {
        try {
            benchmarkExecutor = new BenchmarkExecutor(clusterID, this);
            new Thread(benchmarkExecutor).start();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Creation of benchmark execution client failed: " + e.getLocalizedMessage(), e);
        }
    }

    public void startBftClient() {
        if (bftClient == null && replicaID != null)
            bftClient = new BFTClient(replicaID);
    }

    public void processTransaction(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {

        /*
         TODO: Check if commit data (write operations) is local to cluster or not. If write operations contain even a
         single commit not local to cluster: process transaction?
        */

        MetaInfo metaInfo = localDataVerifier.getMetaInfo(request);

        if (!serializer.serialize(request)) {
            LOGGER.info("Transaction cannot be serialized. Will abort. Request: " + request);
            Bzs.TransactionResponse response =
                    Bzs.TransactionResponse.newBuilder().setStatus(Bzs.TransactionStatus.ABORTED).build();
            if (responseObserver != null) {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                LOGGER.log(Level.WARNING, "Transaction aborted: " + request.toString());
            }
            return;
        }
        sequenceNumber += 1;
        responseHandlerRegistry.addToRegistry(epochNumber, sequenceNumber, request, responseObserver);
        if (metaInfo.remoteRead || metaInfo.remoteWrite) {
            String tid=String.format("%d:%d", epochNumber,sequenceNumber);
            // TODO: Create a remote transaction processor class.

        }
        final int seqNum = sequenceNumber;
        if (seqNum > maxBatchSize) {
            new Thread(() -> resetEpoch(false)).start();
        }
    }

    void resetEpoch(boolean isTimedEpochReset) {
        // Increment Epoch number and reset sequence number.
        synchronized (this) {
            final int seqNumber = sequenceNumber;
            if (isTimedEpochReset && seqNumber < maxBatchSize) {
                return;
            }
            final int epoch = epochNumber;
            serializer.resetEpoch();
            epochNumber += 1;
            sequenceNumber = 0;
            serializer.resetEpoch();
            // Process transactions in the current epoch. Pass the requests gathered during the epoch to BFT Client.
            Map<Integer, Bzs.Transaction> transactions = responseHandlerRegistry.getTransactions(epoch);
            Map<Integer, StreamObserver<Bzs.TransactionResponse>> responseObservers =
                    responseHandlerRegistry.getTransactionObservers(epoch);
            long startTime = 0;
            int transactionCount = 0;
            int processed = 0;
            int failed = 0;
            int bytesProcessed = 0;
            if (transactions != null && transactions.size() > 0) {
                transactionCount = transactions.size();

                LOGGER.info("Processing transaction batch in epoch: " + epoch);

                LOGGER.info("Performing BFT Commit");
                Bzs.TransactionBatch transactionBatch = getTransactionBatch(epoch, transactions.values());
                bytesProcessed = transactionBatch.toByteArray().length;
                startTime = System.currentTimeMillis();
                Bzs.TransactionBatchResponse batchResponse = bftClient.performCommitPrepare(transactionBatch);
                if (batchResponse == null) {
                    failed = transactionCount;
                    sendFailureNotifications(transactions, responseObservers);
                } else {

                    int commitResponseID = bftClient.performDbCommit(batchResponse);
                    if (commitResponseID < 0) {
                        failed = transactionCount;
                        LOGGER.info("DB COMMIT Consensus failed: " + commitResponseID);
                        sendFailureNotifications(transactions, responseObservers);
                    } else {
                        processed = transactionCount;
                        for (int i = 0; i < transactions.size(); i++) {
                            StreamObserver<Bzs.TransactionResponse> responseObserver = responseObservers.get(i + 1);
                            Bzs.TransactionResponse transactionResponse =
                                    batchResponse.getResponses(i);
                            responseObserver.onNext(transactionResponse);
                            responseObserver.onCompleted();
                        }
                    }
                }
            }

            long endTime = System.currentTimeMillis();

            responseHandlerRegistry.clearEpochHistory(epoch);
            if (benchmarkExecutor != null) {
                benchmarkExecutor.logTransactionDetails(
                        epochNumber,
                        transactionCount,
                        processed,
                        failed,
                        startTime,
                        endTime,
                        bytesProcessed);
            }
        }

    }

    private void sendFailureNotifications(Map<Integer, Bzs.Transaction> transactions, Map<Integer,
            StreamObserver<Bzs.TransactionResponse>> responseObservers) {
        LOGGER.info("Received response was null. Transaction failed. Sending response to clients.");
        for (int transactionIndex : transactions.keySet()) {
            StreamObserver<Bzs.TransactionResponse> responseObserver = responseObservers.get(transactionIndex);
            Bzs.TransactionResponse tResponse =
                    Bzs.TransactionResponse.newBuilder().setStatus(Bzs.TransactionStatus.ABORTED).build();
            responseObserver.onNext(tResponse);
            responseObserver.onCompleted();
        }
    }

    Bzs.TransactionBatch getTransactionBatch(int epochNumber, Collection<Bzs.Transaction> transactions) {
        Bzs.TransactionBatch.Builder batchBuilder = Bzs.TransactionBatch.newBuilder();

        for (Bzs.Transaction transaction : transactions) {
            batchBuilder.addTransactions(transaction);
        }
        batchBuilder.setID(epochNumber).setOperation(Bzs.Operation.BFT_PREPARE);
        return batchBuilder.build();
    }
}


class TransactionID implements Comparable<TransactionID>{
    public int epochNumber, sequenceNumber;

    public TransactionID(int epochNumber, int sequenceNumber) {
        this.epochNumber = epochNumber;
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public boolean equals(Object tid) {
        if (tid.getClass().getName().equals(TransactionID.class.getName())) {
            TransactionID objtid = (TransactionID) tid;
            return objtid.epochNumber == epochNumber && objtid.sequenceNumber == sequenceNumber;
        }
        return false;

    }

    @Override
    public int compareTo(TransactionID t2) {
        int eDiff = epochNumber - t2.epochNumber;
        int sDiff = sequenceNumber - t2.sequenceNumber;
        if (eDiff!=0)
            return eDiff;
        return sDiff;
    }
}