package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.ClusterGrpc;
import edu.ucsc.edgelab.db.bzs.data.LockManager;
import edu.ucsc.edgelab.db.bzs.exceptions.InvalidCommitException;
import io.grpc.stub.StreamObserver;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClusterService extends ClusterGrpc.ClusterImplBase {

    private TransactionProcessor processor;
    private Integer replicaID;
    private Integer clusterID;
    private boolean amILeader;
    private Serializer serializer;
    private Map<String, EpochTransactionID> transactionIDMap = new LinkedHashMap<>();
    public static final Logger log = Logger.getLogger(ClusterService.class.getName());


    public ClusterService(Integer clusterID, Integer replicaID, TransactionProcessor processor, boolean isLeader) {
        this.clusterID = clusterID;
        this.replicaID = replicaID;
        this.amILeader = isLeader;
        this.processor = processor;
        serializer = new Serializer(clusterID);
    }

    @Override
    public void commitPrepare(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
        Bzs.TransactionResponse response;
        MetaInfo metaInfo = new LocalDataVerifier(clusterID).getMetaInfo(request);
        String transactionID = request.getTransactionID();
        boolean serializable = serializer.serialize(request);
        if (!serializable) {
            performOperationandSendResponse(transactionID, responseObserver, null, Bzs.TransactionStatus.ABORTED);
        } else {
            LockManager.acquireLocks(request);
            Bzs.Operation operation = Bzs.Operation.BFT_PREPARE;
            int epochNumber = processor.getEpochNumber();
            transactionIDMap.put(transactionID, new EpochTransactionID(epochNumber, request.getTransactionID()));

            Bzs.TransactionBatchResponse batchResponse = null;
            if (metaInfo.localWrite) {

                Bzs.TransactionBatch batch = null;
                try {
                    batch = createTransactionBatch(request, operation);
                    batchResponse = processor.getBFTClient().performCommitPrepare(batch);
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
                performOperationandSendResponse(transactionID, responseObserver, null, Bzs.TransactionStatus.ABORTED);
                LockManager.releaseLocks(request);
            } else {
                response = batchResponse.getResponses(0);
                performOperationandSendResponse(transactionID, responseObserver, response,
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

    public void performOperationandSendResponse(String transactionID,
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
        response = builder
                .setStatus(transactionStatus)
                .setTransactionID(transactionID)
                // call to map is potentially troublesome here
                .setEpochNumber(transactionIDMap.get(transactionID).getEpochNumber())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void commit(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
        Bzs.Operation operation = Bzs.Operation.BFT_COMMIT;
        Bzs.TransactionStatus transactionStatus = Bzs.TransactionStatus.COMMITTED;
        Bzs.TransactionStatus failureStatus = Bzs.TransactionStatus.ABORTED;
        MetaInfo metaInfo = new LocalDataVerifier(clusterID).getMetaInfo(request);
        log.info("Committing transaction request: " + request);
        try {
            if (metaInfo.localWrite) {
                performOperationandSendResponse(request, responseObserver, operation, transactionStatus, failureStatus);
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

    private void performOperationandSendResponse(Bzs.Transaction request,
                                                 StreamObserver<Bzs.TransactionResponse> responseObserver,
                                                 Bzs.Operation operation,
                                                 Bzs.TransactionStatus transactionStatus,
                                                 Bzs.TransactionStatus failureStatus) {
        Bzs.TransactionBatch batch = null;
        try {
            batch = createTransactionBatch(request, operation);
            int status = processor.getBFTClient().dbCommit(batch);
            performOperationandSendResponse(request.getTransactionID(), responseObserver, null,
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
        performOperationandSendResponse(request, responseObserver, Bzs.Operation.BFT_ABORT,
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