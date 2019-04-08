package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.ClusterGrpc;
import io.grpc.stub.StreamObserver;

public class ClusterService extends ClusterGrpc.ClusterImplBase {

    @Override
    public void commit(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {

        super.commit(request, responseObserver);
    }

    @Override
    public void readOperation(Bzs.Read request, StreamObserver<Bzs.ReadResponse> responseObserver) {
        super.readOperation(request, responseObserver);
    }
}
