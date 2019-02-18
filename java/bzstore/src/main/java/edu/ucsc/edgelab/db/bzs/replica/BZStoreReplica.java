package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.ReplicaGrpc;
import io.grpc.stub.StreamObserver;

public class BZStoreReplica extends ReplicaGrpc.ReplicaImplBase {
    @Override
    public void forward(Bzs.TransactionBatch request, StreamObserver<Bzs.TransactionBatchResponse> responseObserver) {
        super.forward(request, responseObserver);
    }
}
