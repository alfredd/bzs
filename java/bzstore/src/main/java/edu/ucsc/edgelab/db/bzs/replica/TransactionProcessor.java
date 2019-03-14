package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import io.grpc.stub.StreamObserver;

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
        // Process transactions in the current epoch. Pass the requests gathered during the epoch to BFT Client.
        Map<Integer, Bzs.Transaction> transactions = responseHandlerRegistry.getTransactions(epoch);
        Map<Integer, StreamObserver<Bzs.TransactionResponse>> responseObservers =
                responseHandlerRegistry.getTransactionObservers(epoch);
        if (transactions != null && transactions.size() > 0) {

            LOGGER.info("Processing transaction batch in epoch: "+epoch);
            BFTClient bftClient = new BFTClient(id);
            LOGGER.info("Performing BFT Commit");
            Bzs.TransactionBatchResponse batchResponse = bftClient.performCommit(transactions.values());
//            LOGGER.info("After commit consensus the response is : "+batchResponse.toString());
            if (batchResponse == null) {
                sendFailureNotifications(transactions, responseObservers);
            } else {
                LOGGER.info("Received response from BFT server cluster. Transaction response is of size "
                        + transactions.size() + ". Performing db commit");
                LOGGER.info("Before DB COMMIT Consensus the data is: "+batchResponse.toString());
                Bzs.TransactionBatchResponse commitResponse = bftClient.performDbCommit(batchResponse);
                if (commitResponse == null) {

                    LOGGER.info("DB COMMIT Consensus failed: "+commitResponse);
                    sendFailureNotifications(transactions, responseObservers);
                    return;
                }
                LOGGER.info("After DB COMMIT Consensus the data is: "+commitResponse.toString());

                for (int i = 0; i < transactions.size(); i++) {
                    StreamObserver<Bzs.TransactionResponse> responseObserver = responseObservers.get(i + 1);
                    Bzs.TransactionResponse transactionResponse = commitResponse.getBftCommitResponse().getResponses(i);
                    LOGGER.info("Processing transaction response: " + transactionResponse.toString());
                    responseObserver.onNext(transactionResponse);
                    responseObserver.onCompleted();

                }
            }
        }/* else {
            LOGGER.info("Nothing to commit in epoch: " + epoch + ". Waiting for next epoch.");
        }*/

        responseHandlerRegistry.clearEpochHistory(epoch);

    }

    private void sendFailureNotifications(Map<Integer, Bzs.Transaction> transactions, Map<Integer,
            StreamObserver<Bzs.TransactionResponse>> responseObservers) {
        LOGGER.info("Received response was null. Transaction failed. Sending response to clients.");
        for (int transactionIndex : transactions.keySet()) {
            StreamObserver<Bzs.TransactionResponse> responseObserver = responseObservers.get(transactionIndex);
            Bzs.TransactionResponse tResponse =
                    Bzs.TransactionResponse.newBuilder().setStatus(Bzs.TransactionStatus.ABORTED).build();
            responseObserver.onNext(tResponse);
            responseObserver.onCompleted();
        }
    }

    void setId(Integer id) {
        this.id = id;
    }
}
