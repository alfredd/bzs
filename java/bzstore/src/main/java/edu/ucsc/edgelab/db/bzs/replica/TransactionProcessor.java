package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import io.grpc.stub.StreamObserver;

public class TransactionProcessor {

    private Serializer serializer;
    private int sequenceNumber;
    private int epochNumber;
    private ResponseHandlerRegistry responseHandlerRegistry;

    public TransactionProcessor() {
        serializer = new Serializer();
        sequenceNumber = 0;
        epochNumber = 0;
        responseHandlerRegistry = new ResponseHandlerRegistry();
    }

    void processTransaction(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
        if (serializer.serialize(request) == false) {

            Bzs.TransactionResponse response = Bzs.TransactionResponse.newBuilder().setStatus(Bzs.TransactionStatus.ABORTED).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }
        responseHandlerRegistry.addToRegistry(epochNumber,sequenceNumber+1,request,responseObserver);
    }
}
