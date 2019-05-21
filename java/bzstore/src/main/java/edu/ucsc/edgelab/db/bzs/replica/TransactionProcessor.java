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
    private Set<TransactionID> remoteOnlyTid = new LinkedHashSet<>();
    private ClusterKeysAccessor clusterKeysAccessor;

    public TransactionProcessor(Integer replicaId, Integer clusterId) {
        this.replicaID = replicaId;
        this.clusterID = clusterId;
        localDataVerifier = new LocalDataVerifier(clusterID);
        serializer = new Serializer(clusterID);
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
        LOGGER.info("Initializing Transaction Processor for server: " + clusterID + " " + replicaID);
        initMaxBatchSize();
        startBftClient();
        EpochManager epochManager = new EpochManager(this);
        epochManager.startEpochMaintenance();
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
            LOGGER.log(Level.WARNING, "Creation of benchmark execution client failed: " + e.getLocalizedMessage(), e);
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
        TransactionID tid = new TransactionID(epochNumber, sequenceNumber);
        Bzs.Transaction transaction = Bzs.Transaction.newBuilder(request).setTransactionID(tid.getTiD()).build();
        LOGGER.info("Transaction assigned with TID: "+tid+", " + transaction);
        if (metaInfo.remoteRead || metaInfo.remoteWrite) {
            remoteOnlyTid.add(tid);
            LOGGER.info("Transaction contains remote operations");
            LockManager.acquireLocks(transaction);
            remoteTransactionProcessor.prepareAsync(tid, transaction);
            responseHandlerRegistry.addToRemoteRegistry(tid, transaction, responseObserver);
        }
        if ( metaInfo.localWrite) {
            LOGGER.info("Transaction contains local write operations");
            remoteOnlyTid.remove(tid);
            responseHandlerRegistry.addToRegistry(epochNumber, sequenceNumber, transaction, responseObserver);
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
            LOGGER.info("Starting remote prepared processing for tid: " + tid);
            if (remoteOnlyTid.contains(tid)) {
                Bzs.Transaction t = responseHandlerRegistry.getRemoteTransaction(tid.getEpochNumber(),
                        tid.getSequenceNumber());
                if (t != null) {
                    remoteTransactionProcessor.commitAsync(tid, t);
                    responseHandlerRegistry.getRemoteTransactionObserver(tid.getEpochNumber(), tid.getSequenceNumber());
                } else
                    LOGGER.log(Level.WARNING, "Could not process remote-only transaction: " + tid);
                return;
            }
            this.remotePreparedList.add(tid);
            int remaining = remotePreparedList.indexOf(tid);
            for (int i = 0; i < remaining; i++) {
                TransactionID tid2 = remotePreparedList.get(i);
                Bzs.Transaction transaction = responseHandlerRegistry.getTransaction(tid2.getEpochNumber(),
                        tid2.getSequenceNumber());
                processRemoteCommits(tid2, transaction);
            }

            Bzs.Transaction t = responseHandlerRegistry.getTransaction(tid.getEpochNumber(), tid.getSequenceNumber());

            boolean executionDone = false;
            if (status.equals(Bzs.TransactionStatus.ABORTED)) {
                sendResponseToClient(tid, status, t);
            } else {
                if (preparedRemoteList.containsKey(tid)) {
                    processRemoteCommits(tid, t);
                    executionDone = true;
                }
            }
            if (!executionDone) {
                remaining -= 1;
            }
            List<TransactionID> removeTidList = new LinkedList<>();

            for (int i = 0; i < remaining; i++)
                removeTidList.add(remotePreparedList.get(i));
            for (int i = 0; i < remaining; i++)
                remotePreparedList.remove(removeTidList.get(i));
            LOGGER.info("Completed remote prepared processing for tid: " + tid);
        }
    }

    private void processRemoteCommits(TransactionID tid, Bzs.Transaction t) {
        Bzs.TransactionBatchResponse batchResponse = preparedRemoteList.get(tid);
        LOGGER.info("Processing remote commits: Tid: " + tid + ". Transaction : " + t);
        if (batchResponse != null) {
            int commitResponse = bftClient.performDbCommit(batchResponse);
            if (commitResponse < 0) {
                remoteTransactionProcessor.abortAsync(tid, t);
                LockManager.releaseLocks(t);
            } else {
                remoteTransactionProcessor.commitAsync(tid, t);
            }
        } else
            LOGGER.info("Local prepare not completed for transaction: Tid: " + tid + ". Transaction : " + t);
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

        Bzs.TransactionResponse response = Bzs.TransactionResponse.newBuilder().setStatus(status).build();
        LOGGER.log(Level.INFO, "Transaction completed with TID" + tid + ", status: " + status + ", response to " +
                "client: " + (response == null ? "null" : response.toString()));
        r.onNext(response);
        r.onCompleted();
        responseHandlerRegistry.removeRemoteTransactions(tid.getEpochNumber(), tid.getSequenceNumber());
        LockManager.releaseLocks(t);
    }


    void resetEpoch(boolean isTimedEpochReset) {
        // Increment Epoch number and reset sequence number.
        synchronized (this) {
            final int seqNumber = sequenceNumber;
            if (!isTimedEpochReset && !(seqNumber < maxBatchSize)) {
                return;
            }
//            LOGGER.info("Epoch number: " + epochNumber + " , Sequence number: "+sequenceNumber);
//            LOGGER.info(String.format("Resetting epoch: %d, sequence numbers: %d", epochNumber, sequenceNumber));
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
            if ((transactions != null && transactions.size() > 0) || (remoteTransactions != null && remoteTransactions.size() > 0)) {
                startTime = System.currentTimeMillis();
                if (transactions != null)
                    transactionCount += transactions.size();

                LOGGER.info("Processing transaction batch in epoch: " + epoch);

                LOGGER.info("Performing BFT Commit");
                if (remoteTransactions != null) {
                    for (Map.Entry<Integer, Bzs.Transaction> entrySet : remoteTransactions.entrySet()) {

                        Bzs.TransactionBatch remoteBatch = Bzs.TransactionBatch.newBuilder()
                                .addTransactions(entrySet.getValue())
                                .setOperation(Bzs.Operation.BFT_PREPARE)
                                .setID(entrySet.getValue().getTransactionID())
                                .build();
                        LOGGER.info("Sending prepare message for remote batch: " + remoteBatch.toString());
                        Bzs.TransactionBatchResponse remoteBatchResponse = performPrepare(remoteBatch);
                        if (remoteBatchResponse != null) {
                            for (Bzs.TransactionResponse response : remoteBatchResponse.getResponsesList()) {
                                preparedRemoteList.put(TransactionID.getTransactionID(remoteBatchResponse.getID()),
                                        remoteBatchResponse);
                            }
                        }
                    }
                }
                LOGGER.info("Local - Distributed Txn prepared. Epoch: "+epoch);
                LOGGER.info("Starting Local Txn batch prepare. Epoch: "+epoch);


                if (transactions != null) {
                    Bzs.TransactionBatch transactionBatch = getTransactionBatch(epoch.toString(),
                            transactions.values());
                    LOGGER.info("Processing transaction batch: " + transactionBatch.toString());

                    Bzs.TransactionBatchResponse batchResponse = performPrepare(transactionBatch);
                    LOGGER.info("Transaction batch size: "+ transactionBatch.getTransactionsCount()
                    +", batch response count: "+batchResponse.getResponsesCount());
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
                            LOGGER.info("Transactions.size = "+transactions.size());
                            int i =0;
//                            for (int i = 0; i < transactions.size(); i++) {
                            for (;i<batchResponse.getResponsesCount();++i) {

                                Bzs.TransactionResponse r = batchResponse.getResponses(i);
                                TransactionID tid = TransactionID.getTransactionID(r.getTransactionID());
//                                Bzs.Transaction t = transactions.get(i);

                                LOGGER.info("Transaction response i = "+i);
//                                Bzs.Transaction t = transactions.get(i);
//                                if (t==null) {
//                                    LOGGER.info("Transaction not found for i="+i);
//                                    continue;
//                                }
//                                TransactionID tid = TransactionID.getTransactionID(t.getTransactionID());
                                LOGGER.info("Transaction tid = "+tid);
                                StreamObserver<Bzs.TransactionResponse> responseObserver = responseHandlerRegistry
                                        .getLocalTransactionObserver(
                                                tid.getEpochNumber(),tid.getSequenceNumber()
                                        );//responseObservers.get(i + 1);
                                LOGGER.info("Response observer is NULL? "+ (responseObserver==null));
                                Bzs.TransactionResponse transactionResponse = batchResponse.getResponses(i);
                                responseObserver.onNext(transactionResponse);
                                responseObserver.onCompleted();
                            }
                        }
                    }
                    bytesProcessed = transactionBatch.toByteArray().length;
                }
                LOGGER.info("Local Txn batch prepared. Epoch: "+epoch);
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

    public static void main(String[] args) throws IOException {
/*        MerkleBTree tree = new MerkleBTree();
        int maxlen = 120;
        byte[][] retHashes = new byte[maxlen][];
        Optional<byte[]>[] rootHashes = new Optional[maxlen];
        for (Integer i = 0; i < maxlen; i++) {
            byte[] rawkey = ("K" + i).getBytes();
            byte[] rawvalue = ("V" + i).getBytes();
            rootHashes[i] = tree.root.hash;

            retHashes[i] = tree.put(rawkey, rawvalue);
        }
        System.out.println(rootHashes[0].equals(rootHashes[2]));

        TreeNode.Nodes nodes = new TreeNode.Nodes();
        byte[] value = tree.get("K65".getBytes(), nodes);
        byte[] valueHash = RAMStorage.hash("V65".getBytes());

        System.out.println("Size root hash: " + nodes.root.length + ", hash" + Arrays.toString(nodes.root));
        System.out.println("Size value hash: " + valueHash.length + ", hash" + Arrays.toString(valueHash));
        System.out.println(new String(value) + ". Hash list size: " + nodes.nodeHash.size());
        System.out.println(Arrays.toString(rootHashes[rootHashes.length - 1].get()));
        for (int i = 0; i < nodes.nodeHash.size(); i++) {
            byte[] treenodeBytes = nodes.nodeHash.get(i);

            System.out.println("Size of node hash: " + treenodeBytes.length + ", " + Arrays.toString(treenodeBytes));

            System.out.println("Length of value Hash: " + valueHash.length
                    + ", length of node value hash: " + treenodeBytes.length);
        }*/
    }

}

