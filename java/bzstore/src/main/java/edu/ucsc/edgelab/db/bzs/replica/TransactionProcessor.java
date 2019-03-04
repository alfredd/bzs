package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import io.grpc.stub.StreamObserver;

public class TransactionProcessor {

    private Serializer serializer;
    private int sequenceNumber;
    private int epochNumber;

    public TransactionProcessor() {
        serializer = new Serializer();
        sequenceNumber = 0;
        epochNumber = 0;
    }

    void processTransaction(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
//        serializer.serialize(request);
    }
}
