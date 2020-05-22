package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.ClusterGrpc;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;
import edu.ucsc.edgelab.db.bzs.data.LockManager;
import edu.ucsc.edgelab.db.bzs.exceptions.InvalidCommitException;
import edu.ucsc.edgelab.db.bzs.txn.*;
import io.grpc.stub.StreamObserver;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClusterService extends ClusterGrpc.ClusterImplBase {

    private TxnProcessor processor;
    private Integer replicaID;
    private Integer clusterID;
    private boolean amILeader;
    private Serializer serializer;
    private Map<String, EpochTransactionID> transactionIDMap = new LinkedHashMap<>();
    private static final Logger log = Logger.getLogger(ClusterService.class.getName());
    private Map<String, ClusterDRWTProcessorImpl> remoteJobProcessorMap = new LinkedHashMap<>();


    public ClusterService(TxnProcessor processor, boolean isLeader) {
        this.clusterID = ID.getClusterID();
        this.replicaID = ID.getReplicaID();
        this.amILeader = isLeader;
        this.processor = processor;
        serializer = new Serializer();
    }


    @Override
    public void commitAll(Bzs.TransactionBatch batchCommitRequest, StreamObserver<Bzs.TransactionBatchResponse> responseObserver) {
//        log.info(String.format("Received batch commit batchCommitRequest: %s", batchCommitRequest.toString()));
        String[] commitRequests = batchCommitRequest.getID().split(",");
        List<ClusterDRWTProcessor> clusterDRWTProcessors = new LinkedList<>();
        int commitSubBatchCounts = commitRequests.length;
        int endIndex = 0;
        for (String commitRequest: commitRequests) {
            String[] ids = commitRequest.split(":");
            String txnID = String.format("%s:%s", ids[0], ids[1]);
            Integer txnCount = Integer.decode(ids[2]);
            ClusterDRWTProcessorImpl clusterDRWTProcessor = remoteJobProcessorMap.get(txnID);

            if (clusterDRWTProcessor == null || (!RemoteTxnCache.isPrepared(clusterDRWTProcessor.getID()))) {
//            log.info(String.format("Transaction batch with Request ID: %s not found in prepared jobs cache. Reqeust: %s", batchCommitRequest.getID(),
//                    batchCommitRequest.toString()));
                sendBatchAbort(batchCommitRequest, responseObserver);
                return;
            }

            Bzs.TransactionBatch.Builder batchBuilder = Bzs.TransactionBatch.newBuilder();
            batchBuilder.setID(txnID);
            int startIndex = endIndex;
            for (; startIndex < endIndex+txnCount; startIndex++) {
                batchBuilder.addResponses(batchCommitRequest.getResponses(startIndex));
            }
            endIndex = startIndex;

            clusterDRWTProcessor.setPhase(ClusterDRWTProcessor.Phase.COMMIT);
            clusterDRWTProcessor.setResponseObserver(responseObserver);
            clusterDRWTProcessor.setRequest(batchBuilder.build());
            clusterDRWTProcessor.commit();
            clusterDRWTProcessors.add(clusterDRWTProcessor);
        }
        boolean notCompleted = true;
        while(notCompleted) {
            int completedCount = 0;
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (ClusterDRWTProcessor processor : clusterDRWTProcessors) {
                if (processor.commitCompleted())
                    completedCount+=1;
            }
            if (completedCount == commitSubBatchCounts) {
                log.info("Completed commit of all txns in "+ batchCommitRequest.getID());
                break;
            }
        }
        responseObserver.onCompleted();
    }

    private void sendBatchAbort(Bzs.TransactionBatch request, StreamObserver<Bzs.TransactionBatchResponse> responseObserver) {
        Bzs.TransactionBatchResponse.Builder responseBuilder = Bzs.TransactionBatchResponse.newBuilder().setID(request.getID());
        for (Bzs.Transaction txn : request.getTransactionsList()) {
            responseBuilder.addResponses(Bzs.TransactionResponse.newBuilder()
                    .setTransactionID(txn.getTransactionID())
                    .setStatus(Bzs.TransactionStatus.FAILURE)
                    .build());
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void prepareAll(Bzs.TransactionBatch request, StreamObserver<Bzs.TransactionBatchResponse> responseObserver) {
        log.info(String.format("Received batch prepare request: %s", request.getID()));
        ClusterDRWTProcessorImpl clusterDRWTProcessor = new ClusterDRWTProcessorImpl(processor);
        clusterDRWTProcessor.setRequest(request);
        clusterDRWTProcessor.setResponseObserver(responseObserver);
        remoteJobProcessorMap.put(request.getID(), clusterDRWTProcessor);
        clusterDRWTProcessor.setPhase(ClusterDRWTProcessor.Phase.PREPARE);
        clusterDRWTProcessor.prepare();
    }

    @Override
    public void abortAll(Bzs.TransactionBatch request, StreamObserver<Bzs.TransactionBatchResponse> responseObserver) {
        super.abortAll(request, responseObserver);
    }


    @Override
    public void commitPrepare(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
        Bzs.TransactionResponse response;
        MetaInfo metaInfo = new LocalDataVerifier().getMetaInfo(request);
        String transactionID = request.getTransactionID();
        boolean serializable = serializer.serialize(request);
        if (!serializable) {
            log.info("Transaction is not serializable: " + request);
            sendResponseToCluster(request.getTransactionID(), responseObserver, Bzs.TransactionStatus.ABORTED,
                    Bzs.TransactionResponse.newBuilder(), 0);
//            performOperationAndSendResponse(transactionID, responseObserver, null, Bzs.TransactionStatus.ABORTED);
        } else {
            LockManager.acquireLocks(request);
            Bzs.Operation operation = Bzs.Operation.BFT_PREPARE;
            int epochNumber = Epoch.getEpochNumber();
            transactionIDMap.put(transactionID, new EpochTransactionID(epochNumber, request.getTransactionID()));

            Bzs.TransactionBatchResponse batchResponse = null;
            if (metaInfo.localWrite) {

                Bzs.TransactionBatch batch = null;
                try {
                    batch = createTransactionBatch(request, operation);
                    batchResponse = BFTClient.getInstance().performCommitPrepare(batch);
//                    log.info("Response of ClusterService Prepare: " + batchResponse);
                } catch (InvalidCommitException e) {
                    log.log(Level.WARNING, e.getLocalizedMessage());
                }
            } else {
                if (metaInfo.localRead) {
                    Bzs.TransactionStatus status = Bzs.TransactionStatus.PREPARED;
                    sendResponse(responseObserver, transactionID, epochNumber, status);
                    return;
                }
            }
            if (batchResponse == null) {
                performOperationAndSendResponse(transactionID, responseObserver, null, Bzs.TransactionStatus.ABORTED);
                LockManager.releaseLocks(request);
            } else {
                response = batchResponse.getResponses(0);
                performOperationAndSendResponse(transactionID, responseObserver, response,
                        Bzs.TransactionStatus.PREPARED);
            }
        }
    }

    private void sendResponse(StreamObserver<Bzs.TransactionResponse> responseObserver, String transactionID,
                              int epochNumber, Bzs.TransactionStatus status) {
        Bzs.TransactionResponse readResponse = getTransactionResponse(transactionID, epochNumber, status);
        responseObserver.onNext(readResponse);
        responseObserver.onCompleted();
    }

    private Bzs.TransactionResponse getTransactionResponse(String transactionID, int epochNumber,
                                                           Bzs.TransactionStatus status) {
        return Bzs.TransactionResponse.newBuilder()
                .setEpochNumber(epochNumber)
                .setTransactionID(transactionID)
                .setStatus(status)
                .build();
    }

    public Bzs.TransactionBatch createTransactionBatch(Bzs.Transaction request, Bzs.Operation operation) throws InvalidCommitException {

        EpochTransactionID epochTransactionID = transactionIDMap.get(request.getTransactionID());
        if (epochTransactionID == null) {
            throw new InvalidCommitException("No mapping found for " + request.getTransactionID());
        }
        return Bzs.TransactionBatch
                .newBuilder()
                .addTransactions(request)
                .setOperation(operation)
                .setID(epochTransactionID.getTransactionID())
                .build();
    }

    public void performOperationAndSendResponse(String transactionID,
                                                StreamObserver<Bzs.TransactionResponse> responseObserver,
                                                Bzs.TransactionResponse templateResponse,
                                                Bzs.TransactionStatus transactionStatus) {
        Bzs.TransactionResponse response;
        Bzs.TransactionResponse.Builder builder;
        if (templateResponse == null)
            builder = Bzs.TransactionResponse.newBuilder();
        else
            builder = Bzs.TransactionResponse.newBuilder(templateResponse);

        sendResponse(transactionID, responseObserver, transactionStatus, builder);
    }

    private void sendResponse(String transactionID, StreamObserver<Bzs.TransactionResponse> responseObserver,
                              Bzs.TransactionStatus transactionStatus, Bzs.TransactionResponse.Builder builder) {
        Bzs.TransactionResponse response;
        int epochNumber = transactionIDMap.get(transactionID).getEpochNumber();
        sendResponseToCluster(transactionID, responseObserver, transactionStatus, builder, epochNumber);
    }

    private void sendResponseToCluster(String transactionID, StreamObserver<Bzs.TransactionResponse> responseObserver
            , Bzs.TransactionStatus transactionStatus, Bzs.TransactionResponse.Builder builder, int epochNumber) {
        Bzs.TransactionResponse response;
        response = builder
                .setStatus(transactionStatus)
                .setTransactionID(transactionID)
                // call to map is potentially troublesome here
                .setEpochNumber(epochNumber)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void commit(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
        Bzs.Operation operation = Bzs.Operation.BFT_COMMIT;
        Bzs.TransactionStatus transactionStatus = Bzs.TransactionStatus.COMMITTED;
        Bzs.TransactionStatus failureStatus = Bzs.TransactionStatus.ABORTED;
        MetaInfo metaInfo = new LocalDataVerifier().getMetaInfo(request);
        log.info("Committing transaction request: " + request);
        try {
            if (metaInfo.localWrite) {
                sendResponse(request, responseObserver, operation, transactionStatus, failureStatus);
            } else {
                if (metaInfo.localRead) {
                    log.info("Transaction request applies only to local reads");
                    Bzs.TransactionStatus status = Bzs.TransactionStatus.PREPARED;
                    String transactionID = request.getTransactionID();
                    int epochNumber = transactionIDMap.get(transactionID).getEpochNumber();
                    sendResponse(responseObserver, transactionID, epochNumber, status);
                }
            }
        } finally {
            LockManager.releaseLocks(request);
        }
    }

    private void sendResponse(Bzs.Transaction request,
                              StreamObserver<Bzs.TransactionResponse> responseObserver,
                              Bzs.Operation operation,
                              Bzs.TransactionStatus transactionStatus,
                              Bzs.TransactionStatus failureStatus) {
        Bzs.TransactionBatch batch = null;
        try {
            batch = createTransactionBatch(request, operation);
            int status = BFTClient.getInstance().dbCommit(batch);
            performOperationAndSendResponse(request.getTransactionID(), responseObserver, null,
                    status < 0 ? failureStatus : transactionStatus);
        } catch (InvalidCommitException e) {
            log.log(Level.WARNING, e.getLocalizedMessage());
            sendResponse(request.getTransactionID(), responseObserver, Bzs.TransactionStatus.ABORTED,
                    Bzs.TransactionResponse.newBuilder());
        }
    }

    /**
     * May have to be removed.
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void readOperation(Bzs.Read request, StreamObserver<Bzs.ReadResponse> responseObserver) {
        super.readOperation(request, responseObserver);
    }

    @Override
    public void abort(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
        LockManager.releaseLocks(request);
        sendResponse(request, responseObserver, Bzs.Operation.BFT_ABORT,
                Bzs.TransactionStatus.ABORTED, Bzs.TransactionStatus.FAILURE);
    }
}


class EpochTransactionID {
    private int epochNumber;
    private String transactionID;

    public EpochTransactionID(int epochNumber, String transactionID) {
        this.epochNumber = epochNumber;
        this.transactionID = transactionID;
    }

    public int getEpochNumber() {
        return epochNumber;
    }

    public String getTransactionID() {
        return epochNumber + ":" + transactionID;
    }
}