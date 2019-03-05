package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import io.grpc.stub.StreamObserver;

import java.util.logging.Level;
import java.util.logging.Logger;

public class TransactionProcessor {

    private Integer epochTimeInMS;
    private Serializer serializer;
    private int sequenceNumber;
    private int epochNumber;
    private ResponseHandlerRegistry responseHandlerRegistry;

    private static final Logger LOGGER = Logger.getLogger(TransactionProcessor.class.getName());

    public TransactionProcessor() {
        serializer = new Serializer();
        sequenceNumber = 0;
        epochNumber = 0;
        responseHandlerRegistry = new ResponseHandlerRegistry();
        try {
            BZStoreProperties properties = new BZStoreProperties();
            this.epochTimeInMS = Integer.decode(properties.getProperty(BZStoreProperties.Configuration.epoch_time_ms));
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Exception occurred while getting epoch time. " + e.getLocalizedMessage());
            this.epochTimeInMS = Configuration.getDefaultEpochTimeInMS();
        }
    }

    void processTransaction(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
        if (serializer.serialize(request) == false) {

            Bzs.TransactionResponse response =
                    Bzs.TransactionResponse.newBuilder().setStatus(Bzs.TransactionStatus.ABORTED).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }
        responseHandlerRegistry.addToRegistry(epochNumber, sequenceNumber + 1, request, responseObserver);
    }
}
