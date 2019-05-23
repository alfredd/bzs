package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
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
    private static final Logger log = Logger.getLogger(TransactionProcessor.class.getName());

    private Integer replicaID;
    private BenchmarkExecutor benchmarkExecutor;
    private BFTClient bftClient = null;
    private RemoteTransactionProcessor remoteTransactionProcessor;
    private List<TransactionID> remotePreparedList;
    private Map<String, Bzs.TransactionBatchResponse> listOfRemoteTransactionsPreparedLocally;
    private Set<TransactionID> remoteOnlyTid = new LinkedHashSet<>();
    private ClusterKeysAccessor clusterKeysAccessor;
    private Set<TransactionID> abortedDistributedTransactions = new LinkedHashSet<>();

    public TransactionProcessor(Integer replicaId, Integer clusterId) {
        this.replicaID = replicaId;
        this.clusterID = clusterId;
        localDataVerifier = new LocalDataVerifier(clusterID);
        serializer = new Serializer(clusterID, replicaId);
        sequenceNumber = 0;
        epochNumber = 0;
        responseHandlerRegistry = new ResponseHandlerRegistry();
        remoteTransactionProcessor = new RemoteTransactionProcessor(clusterID, replicaID);
        remotePreparedList = new LinkedList<>();
        listOfRemoteTransactionsPreparedLocally = new LinkedHashMap<>();
    }

    private void initMaxBatchSize() {
        try {
            BZStoreProperties properties = new BZStoreProperties();
            this.maxBatchSize =
                    Integer.decode(properties.getProperty(BZStoreProperties.Configuration.epoch_batch_size));
        } catch (Exception e) {
            log.log(Level.WARNING,
                    "Exception occurred when getting max batch size from config files: " + e.getLocalizedMessage(), e);
            maxBatchSize = 2000;
        }
        log.info("Maximum BatchSize is set to " + maxBatchSize);
    }

    public void initTransactionProcessor() {
        log.info("Initializing Transaction Processor for server: " + clusterID + " " + replicaID);
        epochNumber = BZDatabaseController.getEpochCount();
        initMaxBatchSize();
        startBftClient();
        EpochManager epochManager = new EpochManager(this);
        epochManager.startEpochMaintenance();
        epochManager.startScheduledTimerTask(new DistributedTxnPreparedProcessorTimerTask(this), "Remote Prepared Processor TimerTask", 1.0);
        initLocalDatabase();
        remoteTransactionProcessor.setObserver(this);
        Timer interClusterConnectorTimer = new Timer("IntraClusterPKIAccessor", true);
        clusterKeysAccessor = new ClusterKeysAccessor(clusterID);
        interClusterConnectorTimer.scheduleAtFixedRate(clusterKeysAccessor, 15, 150 * 1000 * 10);
    }

    private void initLocalDatabase() {
        try {
            benchmarkExecutor = new BenchmarkExecutor(clusterID, this);
            new Thread(benchmarkExecutor).start();
        } catch (IOException e) {
            log.log(Level.WARNING, "Creation of benchmark execution client failed: " + e.getLocalizedMessage(), e);
        }
    }

    private void startBftClient() {
        if (bftClient == null && replicaID != null)
            bftClient = new BFTClient(replicaID);
    }

    public int getEpochNumber() {
        return epochNumber;
    }

    public void processTransaction(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {

        /*
         TODO: Check if commit data (write operations) is local to cluster or not. If write operations contain even a
         single commit not local to cluster: process transaction?
        */

        MetaInfo metaInfo = localDataVerifier.getMetaInfo(request);

        if ((metaInfo.localRead || metaInfo.localWrite) && !serializer.serialize(request)) {
            log.info("Transaction cannot be serialized. Will abort. Request: " + request);
            Bzs.TransactionResponse response =
                    Bzs.TransactionResponse.newBuilder().setStatus(Bzs.TransactionStatus.ABORTED).build();
            if (responseObserver != null) {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                log.log(Level.WARNING, "Transaction aborted: " + request.toString());
            }
            return;
        }
        final TransactionID tid = new TransactionID(epochNumber, sequenceNumber);
        Bzs.Transaction transaction = Bzs.Transaction.newBuilder(request).setTransactionID(tid.getTiD()).build();
        log.info("Transaction assigned with TID: " + tid + ", " + transaction);
        boolean isDistributedTxn = false;
        if (metaInfo.remoteRead || metaInfo.remoteWrite) {
            remoteOnlyTid.add(tid);
            isDistributedTxn = true;
            log.info("Transaction contains remote operations");
            LockManager.acquireLocks(transaction);
            remoteTransactionProcessor.prepareAsync(tid, transaction);
        }
        if (metaInfo.localWrite) {
            log.info("Transaction contains local write operations");
            remoteOnlyTid.remove(tid);
            if (!isDistributedTxn) {
                responseHandlerRegistry.addToRegistry(tid.getEpochNumber(), tid.getSequenceNumber(), transaction, responseObserver);
            } else {
                responseHandlerRegistry.addToRemoteRegistry(tid, transaction, responseObserver);
            }
        }
        sequenceNumber += 1;
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
            log.info("Distributed Txn prepared completed for tid: " + tid + ", with status: " + status);

            /*
                Process Remote-Only transactions and return.
             */
            if (remoteOnlyTid.contains(tid)) {
                log.info("TID: "+ tid+" contains remote-only operations.");

                Bzs.Transaction t = responseHandlerRegistry.getRemoteTransaction(tid.getEpochNumber(), tid.getSequenceNumber());
                if (t != null && status.equals(Bzs.TransactionStatus.PREPARED)) {
                    remoteTransactionProcessor.commitAsync(tid, t);
                    responseHandlerRegistry.getRemoteTransactionObserver(tid.getEpochNumber(), tid.getSequenceNumber());
                } else {
                    log.log(Level.WARNING, "Could not process remote-only transaction: " + tid);
                }
                return;
            }

            if (abortedDistributedTransactions.contains(tid)) {

            }
            Set<TransactionID> completed = startCommitProcessForPreparedTransactions(tid, status);

            log.info("Commit Initiated for " + completed);
        }
    }

    public int getRemotePreparedListSize() {
        return remotePreparedList.size();
    }

    public Set<TransactionID> startCommitProcessForPreparedTransactions(TransactionID tid, Bzs.TransactionStatus status) {
    /*
        Process distributed transactions starting with all transactions that have already been prepared.
     */
        int remaining = remotePreparedList.size();
        if (tid != null && status.equals(Bzs.TransactionStatus.PREPARED)) {
            this.remotePreparedList.add(tid);
            remaining = remotePreparedList.size();
        }
        log.info("Remaining TIDs in remotePreparedList= " + remotePreparedList);
        Set<TransactionID> completed = new HashSet<>();
        for (int i = 0; i < remaining; i++) {
            TransactionID tid2 = remotePreparedList.get(i);
            Bzs.Transaction transaction = responseHandlerRegistry.getRemoteTransaction(tid2.getEpochNumber(), tid2.getSequenceNumber());
//            log.info("Processing distributed transaction commit.");
            boolean commitInitiated = processRemoteCommits(tid2, transaction);
            if (commitInitiated)
                completed.add(tid2);
        }

            /*
                Process current transaction.
             */
        if (tid != null) {
            Bzs.Transaction t = responseHandlerRegistry.getRemoteTransaction(tid.getEpochNumber(), tid.getSequenceNumber());
            if (t != null) {
                boolean executionDone = false;
                if (status.equals(Bzs.TransactionStatus.ABORTED)) {
                    log.info("Sending message to clients for aborted transaction: " + tid + ", " + t);
                    sendResponseToClient(tid, status, t);
                } else {
                    executionDone = processRemoteCommits(tid, t);
                    if (executionDone) {
                        completed.add(tid);
                    }
                }
            } else {
                log.info("Transaction with TID: " + tid + " has not yet been prepared locally.");
            }
        }

            /*
                Remove processed transactions from remotePreparedList
             */

        for (TransactionID id : completed) {
            remotePreparedList.remove(id);
        }
        return completed;
    }

    private boolean processRemoteCommits(TransactionID tid, Bzs.Transaction t) {
        boolean status = true;
        log.info("List of listOfRemoteTransactionsPreparedLocally: "+ listOfRemoteTransactionsPreparedLocally);
        Bzs.TransactionBatchResponse batchResponse = listOfRemoteTransactionsPreparedLocally.get(tid.getTiD());
        log.info("Processing remote commits: Tid: " + tid);
        if (batchResponse != null) {
            int commitResponse = bftClient.performDbCommit(batchResponse);

            if (commitResponse < 0) {
                remoteTransactionProcessor.abortAsync(tid, t);
                LockManager.releaseLocks(t);
            } else {
                remoteTransactionProcessor.commitAsync(tid, t);
            }
        } else {
            status = false;
            log.info("Local prepare not completed for transaction: Tid: " + tid);
        }
        return status;
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
        if (r==null) {
            log.log(Level.WARNING, "Trying to send response to a client that does not exist in the response handler registry.");
            return;
        }
        Bzs.TransactionResponse response = Bzs.TransactionResponse.newBuilder().setStatus(status).build();
        log.log(Level.INFO, "Transaction completed with TID" + tid + ", status: " + status + ", response to " +
                "client: " + (response == null ? "null" : response.toString()));
        r.onNext(response);
        r.onCompleted();
        log.info("Sent transaction response to client.");
        responseHandlerRegistry.removeRemoteTransactions(tid.getEpochNumber(), tid.getSequenceNumber());
        log.info("Removed tid"+tid+" from Epoch history.");
        LockManager.releaseLocks(t);
        log.info("Released locks held for the transaction.");
    }


    void resetEpoch(boolean isTimedEpochReset) {
        // Increment Epoch number and reset sequence number.
        synchronized (this) {
            final int seqNumber = sequenceNumber;
            if (!isTimedEpochReset && !(seqNumber < maxBatchSize)) {
                return;
            }
//            log.info(String.format("Resetting epoch: %d, sequence numbers: %d", epochNumber, sequenceNumber));
            final Integer epoch = epochNumber;
            Map<Integer, Bzs.Transaction> transactions = responseHandlerRegistry.getLocalTransactions(epoch);

            Map<Integer, Bzs.Transaction> remoteTransactions = responseHandlerRegistry.getRemoteTransactions(epoch);
            if (transactions == null && remoteTransactions == null) {
                return;
            }
            log.info("Epoch number: " + epoch + " , Sequence number: " + sequenceNumber);
            epochNumber += 1;
            BZDatabaseController.setEpochCount(epochNumber);
            sequenceNumber = 0;
            serializer.resetEpoch();
            // Process transactions in the current epoch. Pass the requests gathered during the epoch to BFT Client.

            Map<Integer, StreamObserver<Bzs.TransactionResponse>> responseObservers = responseHandlerRegistry.getLocalTransactionObservers(epoch);
            long startTime = 0;
            int transactionCount = 0;
            int processed = 0;
            int failed = 0;
            int bytesProcessed = 0;
            if ((transactions != null && transactions.size() > 0) || (remoteTransactions != null && remoteTransactions.size() > 0)) {
                startTime = System.currentTimeMillis();
                if (transactions != null)
                    transactionCount += transactions.size();

                log.info("Processing transaction batch in epoch: " + epoch);

                log.info("Performing BFT Commit");
                if (remoteTransactions != null) {
                    for (Map.Entry<Integer, Bzs.Transaction> entrySet : remoteTransactions.entrySet()) {

                        Bzs.TransactionBatch remoteBatch = Bzs.TransactionBatch.newBuilder()
                                .addTransactions(entrySet.getValue())
                                .setOperation(Bzs.Operation.BFT_PREPARE)
                                .setID(entrySet.getValue().getTransactionID())
                                .build();
                        log.info("Sending prepare message for remote batch: " + remoteBatch.toString());
                        Bzs.TransactionBatchResponse remoteBatchResponse = performPrepare(remoteBatch);
                        if (remoteBatchResponse != null) {
                            log.info("Local prepare completed for distributed transaction: " + remoteBatchResponse);
                            for (Bzs.TransactionResponse response : remoteBatchResponse.getResponsesList()) {
                                log.info("Adding responses to list listOfRemoteTransactionsPreparedLocally: " + response);
                                listOfRemoteTransactionsPreparedLocally.put(TransactionID.getTransactionID(remoteBatchResponse.getID()).getTiD(),
                                        remoteBatchResponse);
                            }
                        } else {
                            abortedDistributedTransactions.add(TransactionID.getTransactionID(entrySet.getValue().getTransactionID()));
                            log.log(Level.WARNING, "Remote transaction could not be prepared locally: " + remoteBatch.toString() + ". Transactions " +
                                    "part of this batch will be aborted.");
                        }
                    }
                }
                log.info("Local - Distributed Txn prepared. Epoch: " + epoch);
                log.info("Starting Local Txn batch prepare. Epoch: " + epoch);


                if (transactions != null) {
                    Bzs.TransactionBatch transactionBatch = getTransactionBatch(epoch.toString(), transactions.values());
                    log.info("Processing transaction batch: " + transactionBatch.toString());

                    Bzs.TransactionBatchResponse batchResponse = performPrepare(transactionBatch);
                    log.info("Transaction batch size: " + transactionBatch.getTransactionsCount() + ", batch response count: " + batchResponse.getResponsesCount());
                    if (batchResponse == null) {
                        failed = transactionCount;
                        sendFailureNotifications(transactions, responseObservers);
                    } else {

                        int commitResponseID = bftClient.performDbCommit(batchResponse);
                        if (commitResponseID < 0) {
                            failed = transactionCount;
                            log.info("DB COMMIT Consensus failed: " + commitResponseID);
                            sendFailureNotifications(transactions, responseObservers);
                        } else {
                            processed = transactionCount;
                            log.info("Transactions.size = " + transactions.size());
                            int i = 0;
//                            for (int i = 0; i < transactions.size(); i++) {
                            for (; i < batchResponse.getResponsesCount(); ++i) {

                                Bzs.TransactionResponse r = batchResponse.getResponses(i);
                                TransactionID tid = TransactionID.getTransactionID(r.getTransactionID());
//                                Bzs.Transaction t = transactions.get(i);

                                log.info("Transaction response i = " + i);
//                                Bzs.Transaction t = transactions.get(i);
//                                if (t==null) {
//                                    log.info("Transaction not found for i="+i);
//                                    continue;
//                                }
//                                TransactionID tid = TransactionID.getTransactionID(t.getTransactionID());
                                log.info("Transaction tid = " + tid);
                                StreamObserver<Bzs.TransactionResponse> responseObserver = responseHandlerRegistry
                                        .getLocalTransactionObserver(
                                                tid.getEpochNumber(), tid.getSequenceNumber()
                                        );//responseObservers.get(i + 1);
                                log.info("Response observer is NULL? " + (responseObserver == null));
                                Bzs.TransactionResponse transactionResponse = batchResponse.getResponses(i);
                                responseObserver.onNext(transactionResponse);
                                responseObserver.onCompleted();
                            }
                        }
                    }
                    bytesProcessed = transactionBatch.toByteArray().length;
                }
                log.info("Local Txn batch prepared. Epoch: " + epoch);
            }

            long endTime = System.currentTimeMillis();

            responseHandlerRegistry.clearLocalHistory(epoch);
            if (benchmarkExecutor != null) {
                benchmarkExecutor.logTransactionDetails(
                        epoch,
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
        log.info("Received response was null. Transaction failed. Sending response to clients.");
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

