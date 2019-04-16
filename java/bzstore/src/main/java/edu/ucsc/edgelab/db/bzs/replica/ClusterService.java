package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.ClusterGrpc;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;
import io.grpc.stub.StreamObserver;

public class ClusterService extends ClusterGrpc.ClusterImplBase {

    private TransactionProcessor processor;
    private Integer replicaID;
    private Integer clusterID;
    private boolean amILeader;
    private Serializer serializer = new Serializer();

    private BFTClient bftClient;

    public ClusterService() {
    }

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
            sendResponse(transactionID, responseObserver, null, Bzs.TransactionStatus.ABORTED);
        } else {
            Bzs.TransactionBatch batch = Bzs.TransactionBatch.newBuilder().addTransactions(request).build();
            Bzs.TransactionBatchResponse batchResponse = processor.getBFTClient().performCommitPrepare(batch);
            if (batchResponse == null) {
                sendResponse(transactionID,responseObserver,null, Bzs.TransactionStatus.ABORTED);
            } else {
                response = batchResponse.getResponses(0);
                sendResponse(transactionID,responseObserver,response, Bzs.TransactionStatus.PREPARED);
            }
        }
    }

    public void sendResponse(String transactionID,
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
        super.commit(request, responseObserver);
    }

    @Override
    public void readOperation(Bzs.Read request, StreamObserver<Bzs.ReadResponse> responseObserver) {
        super.readOperation(request, responseObserver);
    }
}
