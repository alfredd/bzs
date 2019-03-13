package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.exceptions.InvalidCommitException;
import io.grpc.stub.StreamObserver;

import java.util.LinkedList;
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

    }

    public void startEpochMaintainer() {
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
        serializer.resetEpoch();
        synchronized (this) {
            epochNumber += 1;
            sequenceNumber = 0;
            serializer.resetEpoch();
        }
        LOGGER.info("Epoch reset begins for "+epoch);
        // Process transactions in the current epoch. Pass the requests gathered during the epoch to BFT Client.
        Map<Integer, Bzs.Transaction> transactions = responseHandlerRegistry.getTransactions(epoch);
        Map<Integer, StreamObserver<Bzs.TransactionResponse>> responseObservers =
                responseHandlerRegistry.getTransactionObservers(epoch);
        if (transactions != null && transactions.size() > 0) {
            LOGGER.info("Processing transaction batch.");
            BFTClient bftClient = new BFTClient(id);
            LOGGER.info("Performing BFT Commit");
            List<Bzs.TransactionResponse> transactionResponses = bftClient.performCommit(transactions.values());
            if (transactionResponses == null) {
                LOGGER.info("Received response was null. Transaction failed. Sending response to clients.");
                for (int transactionIndex : transactions.keySet()) {
                    StreamObserver<Bzs.TransactionResponse> responseObserver = responseObservers.get(transactionIndex);
                    Bzs.TransactionResponse tResponse =
                            Bzs.TransactionResponse.newBuilder().setStatus(Bzs.TransactionStatus.ABORTED).build();
                    responseObserver.onNext(tResponse);
                    responseObserver.onCompleted();
                }
            } else {
                LOGGER.info("Received response from BFT server cluster. Transaction response is of size "+transactions.size());
                for (int i = 0; i < transactions.size(); i++) {
                    StreamObserver<Bzs.TransactionResponse> responseObserver = responseObservers.get(i+1);
                    Bzs.TransactionResponse transactionResponse = transactionResponses.get(i);
                    LOGGER.info("Processing transaction response: "+ transactionResponse.toString());

                    commitAndSendResponse(responseObserver, transactionResponse);
                }
            }
        } else {
            LOGGER.info("Nothing to commit in epoch: " + epoch + ". Waiting for next epoch.");
        }

        responseHandlerRegistry.clearEpochHistory(epoch);

    }

    void commitAndSendResponse(StreamObserver<Bzs.TransactionResponse> responseObserver,
                               Bzs.TransactionResponse transactionResponse) {

        LOGGER.info("Committing transaction: " + transactionResponse);
        List<String> committedKeys = new LinkedList<>();
        boolean committed = true;
        for (Bzs.WriteResponse writeResponse : transactionResponse.getWriteResponsesList()) {
            BZStoreData data = new BZStoreData(
                    writeResponse.getValue(),
                    writeResponse.getVersion(),
                    writeResponse.getResponseDigest());
            try {
                LOGGER.info("Committing write : " + writeResponse);
                BZDatabaseController.commit(writeResponse.getKey(), data);
                committedKeys.add(writeResponse.getKey());
                LOGGER.info("Committed write : " + writeResponse);

            } catch (InvalidCommitException e) {
                LOGGER.log(Level.WARNING, "Commit failed for transaction: " + transactionResponse.toString() + ". All" +
                        " committed keys will be rolled back and the transaction will be aborted.");
                BZDatabaseController.rollbackForKeys(committedKeys);
                responseObserver.onNext(
                        Bzs.TransactionResponse.newBuilder(transactionResponse)
                                .setStatus(Bzs.TransactionStatus.ABORTED)
                                .build());
                committed = false;
                break;
            }
        }
        if (committed) {
            LOGGER.info("Adding transaction response to observer.");
            responseObserver.onNext(transactionResponse);
        }
        LOGGER.info("Sending response.");
        responseObserver.onCompleted();
    }

    void setId(Integer id) {
        this.id = id;
    }
}
