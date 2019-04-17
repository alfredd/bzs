package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.ClusterGrpc;
import edu.ucsc.edgelab.db.bzs.data.LockManager;
import io.grpc.stub.StreamObserver;

public class ClusterService extends ClusterGrpc.ClusterImplBase {

    private TransactionProcessor processor;
    private Integer replicaID;
    private Integer clusterID;
    private boolean amILeader;
    private Serializer serializer = new Serializer();


    public ClusterService(Integer clusterID, Integer replicaID, TransactionProcessor processor, boolean isLeader) {
        this.clusterID = clusterID;
        this.replicaID = replicaID;
        this.amILeader = isLeader;
        this.processor = processor;
    }

    @Override
    public void commitPrepare(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
        Bzs.TransactionResponse response;
        String transactionID = request.getTransactionID();
        boolean serializable = serializer.serialize(request);
        if (!serializable) {
            performOperationandSendResponse(transactionID, responseObserver, null, Bzs.TransactionStatus.ABORTED);
        } else {
            LockManager.acquireLocks(request);
            Bzs.Operation operation = Bzs.Operation.BFT_PREPARE;
            Bzs.TransactionBatch batch = createTransactionBatch(request, operation);
            Bzs.TransactionBatchResponse batchResponse = processor.getBFTClient().performCommitPrepare(batch);
            if (batchResponse == null) {
                performOperationandSendResponse(transactionID, responseObserver, null, Bzs.TransactionStatus.ABORTED);
            } else {
                response = batchResponse.getResponses(0);
                performOperationandSendResponse(transactionID, responseObserver, response,
                        Bzs.TransactionStatus.PREPARED);
            }
        }
    }

    public Bzs.TransactionBatch createTransactionBatch(Bzs.Transaction request, Bzs.Operation operation) {

        return Bzs.TransactionBatch
                .newBuilder()
                .addTransactions(request)
                .setOperation(operation)
                .setID(request.getTransactionID())
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

        response = builder
                .setStatus(transactionStatus)
                .setTransactionID(transactionID)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void commit(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
        Bzs.Operation operation = Bzs.Operation.BFT_COMMIT;
        Bzs.TransactionStatus transactionStatus = Bzs.TransactionStatus.COMMITTED;
        Bzs.TransactionStatus failureStatus = Bzs.TransactionStatus.ABORTED;
        performOperationandSendResponse(request, responseObserver, operation, transactionStatus, failureStatus);
        LockManager.releaseLocks(request);
    }

    private void performOperationandSendResponse(Bzs.Transaction request,
                                                 StreamObserver<Bzs.TransactionResponse> responseObserver,
                                                 Bzs.Operation operation,
                                                 Bzs.TransactionStatus transactionStatus,
                                                 Bzs.TransactionStatus failureStatus) {
        Bzs.TransactionBatch batch = createTransactionBatch(request, operation);
        int status = processor.getBFTClient().dbCommit(batch);
        performOperationandSendResponse(request.getTransactionID(), responseObserver, null,
                status < 0 ? failureStatus : transactionStatus);
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
        performOperationandSendResponse(request, responseObserver, Bzs.Operation.BFT_ABORT,
                Bzs.TransactionStatus.ABORTED, Bzs.TransactionStatus.FAILURE);
    }
}
