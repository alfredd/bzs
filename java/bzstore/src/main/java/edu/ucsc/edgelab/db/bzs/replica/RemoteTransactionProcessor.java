package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import io.grpc.stub.StreamObserver;

public class RemoteTransactionProcessor  {


    public void processAsync (String tid, Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
    }
}
