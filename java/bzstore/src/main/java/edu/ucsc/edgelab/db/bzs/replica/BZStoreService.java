package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.BZStoreGrpc;
import edu.ucsc.edgelab.db.bzs.Bzs;
import io.grpc.stub.StreamObserver;

class BZStoreService extends BZStoreGrpc.BZStoreImplBase {

    @Override
    public void commit(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
        super.commit(request, responseObserver);
    }

    @Override
    public void rOCommit(Bzs.ROTransaction request, StreamObserver<Bzs.ROTransactionResponse> responseObserver) {
        super.rOCommit(request, responseObserver);
    }

    @Override
    public void readOperation(Bzs.Read request, StreamObserver<Bzs.ReadResponse> responseObserver) {
        super.readOperation(request, responseObserver);
    }
}
