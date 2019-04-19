package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.data.LockManager;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.*;
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
    private RemoteTransactionProcessor remoteTransactionProcessor;
    private List<TransactionID> remotePreparedList;
    private Map<TransactionID, Bzs.TransactionBatchResponse> preparedRemoteList;

    public TransactionProcessor(Integer replicaId, Integer clusterId) {
        this.replicaID = replicaId;
        this.clusterID = clusterId;
        localDataVerifier = new LocalDataVerifier(clusterID);
        serializer = new Serializer();
        sequenceNumber = 0;
        epochNumber = 0;
        responseHandlerRegistry = new ResponseHandlerRegistry();
        remoteTransactionProcessor = new RemoteTransactionProcessor(clusterID, replicaID);
        remotePreparedList = new LinkedList<>();
        preparedRemoteList = new LinkedHashMap<>();
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
        remoteTransactionProcessor.setObserver(this);
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
        TransactionID tid = new TransactionID(epochNumber, sequenceNumber);
        Bzs.Transaction transaction = Bzs.Transaction.newBuilder(request).setTransactionID(tid.getTiD()).build();
        if (metaInfo.remoteRead || metaInfo.remoteWrite) {
            // TODO: Create a remote transaction processor class.
            LockManager.acquireLocks(transaction);
            remoteTransactionProcessor.prepareAsync(tid, transaction);
            responseHandlerRegistry.addToRemoteRegistry(tid, transaction, responseObserver);
        } else {
            responseHandlerRegistry.addToRegistry(epochNumber, sequenceNumber, transaction, responseObserver);
        }
        final int seqNum = sequenceNumber;
        if (seqNum > maxBatchSize) {
            new Thread(() -> resetEpoch(false)).start();
        }
    }

    /**
     * Callback from @{@link RemoteTransactionProcessor}
     *
     * @param tid
     * @param status
     */
    void prepareOperationObserver(TransactionID tid, Bzs.TransactionStatus status) {
        synchronized (this) {

            this.remotePreparedList.add(tid);
            int remaining = remotePreparedList.indexOf(tid);
            for (int i =0;i<remaining;i++) {
                TransactionID tid2 = remotePreparedList.get(i);
                Bzs.Transaction transaction = responseHandlerRegistry.getTransaction(tid2.getEpochNumber(), tid2.getSequenceNumber());
                processRemoteCommits(tid2,transaction);
            }

            Bzs.Transaction t = responseHandlerRegistry.getTransaction(tid.getEpochNumber(), tid.getSequenceNumber());

            boolean executionDone=false;
            if (status.equals(Bzs.TransactionStatus.ABORTED)) {
                sendResponseToClient(tid, status, t);
            } else {
                if (preparedRemoteList.containsKey(tid)) {
                    processRemoteCommits(tid, t);
                    executionDone=true;
                }
            }
            if (executionDone) {
                remaining+=1;
            }
            for (int i =0;i<remaining;i++)
                remotePreparedList.remove(i);
        }
    }

    private void processRemoteCommits(TransactionID tid, Bzs.Transaction t) {
        int commitResponse = bftClient.performDbCommit(preparedRemoteList.get(tid));
        if (commitResponse < 0) {
            remoteTransactionProcessor.abortAsync(tid, t);
            LockManager.releaseLocks(t);
        } else
            remoteTransactionProcessor.commitAsync(tid, t);
    }

    void commitOperationObserver(TransactionID tid, Bzs.TransactionStatus status) {
//        this.remotePreparedList.add(tid);
        Bzs.Transaction t = responseHandlerRegistry.getTransaction(tid.getEpochNumber(), tid.getSequenceNumber());
        if (!status.equals(Bzs.TransactionStatus.COMMITTED))
            remoteTransactionProcessor.abortAsync(tid, t);
        sendResponseToClient(tid, status, t);
        LockManager.releaseLocks(t);

    }

    public void abortOperationObserver(TransactionID tid, Bzs.TransactionStatus transactionStatus) {
        Bzs.Transaction t = responseHandlerRegistry.getTransaction(tid.getEpochNumber(), tid.getSequenceNumber());
        LockManager.releaseLocks(t);
        remotePreparedList.remove(tid);
    }

    private void sendResponseToClient(TransactionID tid, Bzs.TransactionStatus status, Bzs.Transaction t) {
        StreamObserver<Bzs.TransactionResponse> r =
                responseHandlerRegistry.getRemoteTransactionObserver(tid.getEpochNumber(), tid.getSequenceNumber());
        LOGGER.log(Level.WARNING, "Aborting transaction " + tid);

        Bzs.TransactionResponse response = Bzs.TransactionResponse.newBuilder().setStatus(status).build();
        r.onNext(response);
        r.onCompleted();
        responseHandlerRegistry.removeRemoteTransactions(tid.getEpochNumber(), tid.getSequenceNumber());
        LockManager.releaseLocks(t);
    }


    void resetEpoch(boolean isTimedEpochReset) {
        // Increment Epoch number and reset sequence number.
        synchronized (this) {
            final int seqNumber = sequenceNumber;
            if (isTimedEpochReset && seqNumber < maxBatchSize) {
                return;
            }
            final Integer epoch = epochNumber;
            serializer.resetEpoch();
            epochNumber += 1;
            sequenceNumber = 0;
            serializer.resetEpoch();
            // Process transactions in the current epoch. Pass the requests gathered during the epoch to BFT Client.
            Map<Integer, Bzs.Transaction> transactions = responseHandlerRegistry.getLocalTransactions(epoch);

            Map<Integer, Bzs.Transaction> remoteTransactions = responseHandlerRegistry.getRemoteTransactions(epoch);

            Map<Integer, StreamObserver<Bzs.TransactionResponse>> responseObservers =
                    responseHandlerRegistry.getLocalTransactionObservers(epoch);
            long startTime = 0;
            int transactionCount = 0;
            int processed = 0;
            int failed = 0;
            int bytesProcessed = 0;
            if (transactions != null && transactions.size() > 0) {
                startTime = System.currentTimeMillis();
                transactionCount = transactions.size();

                LOGGER.info("Processing transaction batch in epoch: " + epoch);

                LOGGER.info("Performing BFT Commit");
                Bzs.TransactionBatch transactionBatch = getTransactionBatch(epoch.toString(), transactions.values());

                for (Map.Entry<Integer, Bzs.Transaction> entrySet : remoteTransactions.entrySet()) {

                    Bzs.TransactionBatchResponse remoteBatchResponse =
                            performPrepare(Bzs.TransactionBatch.newBuilder()
                                    .addTransactions(entrySet.getValue())
                                    .setOperation(Bzs.Operation.BFT_PREPARE)
                                    .setID(entrySet.getValue().getTransactionID())
                                    .build());
                    if (remoteBatchResponse != null) {
                        for (Bzs.TransactionResponse response : remoteBatchResponse.getResponsesList()) {
                            preparedRemoteList.put(TransactionID.getTransactionID(remoteBatchResponse.getID()),
                                    remoteBatchResponse);
                        }
                    }
                }


                Bzs.TransactionBatchResponse batchResponse = performPrepare(transactionBatch);
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
                bytesProcessed = transactionBatch.toByteArray().length;
            }

            long endTime = System.currentTimeMillis();

            responseHandlerRegistry.clearLocalHistory(epoch);
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

    public Bzs.TransactionBatchResponse performPrepare(Bzs.TransactionBatch transactionBatch) {
        return bftClient.performCommitPrepare(transactionBatch);
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

    Bzs.TransactionBatch getTransactionBatch(String id, Collection<Bzs.Transaction> transactions) {
        Bzs.TransactionBatch.Builder batchBuilder = Bzs.TransactionBatch.newBuilder();

        for (Bzs.Transaction transaction : transactions) {
            batchBuilder.addTransactions(transaction);
        }
        batchBuilder.setID(id.toString()).setOperation(Bzs.Operation.BFT_PREPARE);
        return batchBuilder.build();
    }

    public BFTClient getBFTClient() {
        return bftClient;
    }


}

