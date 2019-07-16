package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import io.grpc.stub.StreamObserver;

public interface TransactionProcessorINTF {
    void processTransaction(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver);
}
