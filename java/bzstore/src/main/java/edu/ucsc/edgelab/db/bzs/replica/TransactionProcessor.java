package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import io.grpc.stub.StreamObserver;

public class TransactionProcessor {

    private Serializer serializer;

    public TransactionProcessor() {
        serializer = new Serializer();
    }

    void processTransaction(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
//        serializer.serialize(request);
    }
}
