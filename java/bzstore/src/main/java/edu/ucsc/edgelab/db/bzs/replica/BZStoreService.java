package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.BZStoreGrpc;
import edu.ucsc.edgelab.db.bzs.Commit;
import io.grpc.stub.StreamObserver;

class BZStoreService extends BZStoreGrpc.BZStoreImplBase {

    @Override
    public void commit(Commit.Transaction request, StreamObserver<Commit.TransactionResponse> responseObserver) {
        super.commit(request, responseObserver);
    }

    @Override
    public void rOCommit(Commit.ROTransaction request, StreamObserver<Commit.ROTransactionResponse> responseObserver) {
        super.rOCommit(request, responseObserver);
    }

    @Override
    public void readOperation(Commit.Read request, StreamObserver<Commit.ReadResponse> responseObserver) {
        super.readOperation(request, responseObserver);
    }
}
