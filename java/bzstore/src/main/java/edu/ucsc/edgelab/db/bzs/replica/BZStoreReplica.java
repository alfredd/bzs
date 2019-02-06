package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Commit;
import edu.ucsc.edgelab.db.bzs.ReplicaGrpc;
import io.grpc.stub.StreamObserver;

public class BZStoreReplica extends ReplicaGrpc.ReplicaImplBase {
    @Override
    public void forward(Commit.TransactionBatch request, StreamObserver<Commit.TransactionBatchResponse> responseObserver) {
        super.forward(request, responseObserver);
    }
}
