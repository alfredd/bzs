package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.exceptions.InvalidCommitException;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TransactionProcessor {

    private Integer epochTimeInMS;
    private Serializer serializer;
    private int sequenceNumber;
    private int epochNumber;
    private ResponseHandlerRegistry responseHandlerRegistry;

    private static final Logger LOGGER = Logger.getLogger(TransactionProcessor.class.getName());
    private Integer id;

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

        EpochMaintainer epochMaintainer = new EpochMaintainer();
        epochMaintainer.setProcessor(this);
        Timer epochTimer = new Timer("EpochMaintainer", true);
        epochTimer.scheduleAtFixedRate(epochMaintainer, epochTimeInMS, epochTimeInMS);
    }

    void processTransaction(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
        if (!serializer.serialize(request)) {

            Bzs.TransactionResponse response =
                    Bzs.TransactionResponse.newBuilder().setStatus(Bzs.TransactionStatus.ABORTED).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }
        synchronized (this) {
            sequenceNumber += 1;
            responseHandlerRegistry.addToRegistry(epochNumber, sequenceNumber, request, responseObserver);
        }
    }

    void resetEpoch() {
        // Increment Epoch number and reset sequence number.
        int epoch = epochNumber;
        synchronized (this) {
            epochNumber += 1;
            sequenceNumber = 0;
            serializer.resetEpoch();
        }
        // Process transactions in the current epoch. Pass the requests gathered during the epoch to BFT Client.
        Map<Integer, Bzs.Transaction> transactions = responseHandlerRegistry.getTransactions(epoch);
        Map<Integer, StreamObserver<Bzs.TransactionResponse>> responseObservers =
                responseHandlerRegistry.getTransactionObservers(epoch);
        BFTClient bftClient = new BFTClient(id);

        List<Bzs.TransactionResponse> transactionResponses = bftClient.performCommit(transactions.values());
        if (transactionResponses == null) {
            for (int transactionIndex : transactions.keySet()) {
                StreamObserver<Bzs.TransactionResponse> responseObserver = responseObservers.get(transactionIndex);
                Bzs.TransactionResponse tResponse =
                        Bzs.TransactionResponse.newBuilder().setStatus(Bzs.TransactionStatus.ABORTED).build();
                responseObserver.onNext(tResponse);
                responseObserver.onCompleted();
            }
        } else {
            for (int i = 0; i < transactions.size(); i++) {
                StreamObserver<Bzs.TransactionResponse> responseObserver = responseObservers.get(i);
                Bzs.TransactionResponse transactionResponse = transactionResponses.get(i);

                commitAndSendResponse(responseObserver, transactionResponse);
            }
        }

        responseHandlerRegistry.clearEpochHistory(epoch);

    }

    void commitAndSendResponse(StreamObserver<Bzs.TransactionResponse> responseObserver,
                               Bzs.TransactionResponse transactionResponse) {
        for (Bzs.WriteResponse writeResponse : transactionResponse.getWriteResponsesList()) {
            BZStoreData data = new BZStoreData(
                    writeResponse.getValue(),
                    writeResponse.getVersion(),
                    writeResponse.getResponseDigest());
            try {
                BZDatabaseController.commit(writeResponse.getKey(), data);
                responseObserver.onNext(transactionResponse);
            } catch (InvalidCommitException e) {
                responseObserver.onNext(
                        Bzs.TransactionResponse.newBuilder(transactionResponse)
                                .setStatus(Bzs.TransactionStatus.ABORTED)
                                .build());
            }
        }
        responseObserver.onCompleted();
    }

    void setId(Integer id) {
        this.id = id;
    }
}
