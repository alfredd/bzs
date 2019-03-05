package edu.ucsc.edgelab.db.bzs.replica;

import com.google.common.collect.Lists;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
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
        epochTimer.scheduleAtFixedRate(epochMaintainer,epochTimeInMS,epochTimeInMS);
    }

    void processTransaction(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
        if (serializer.serialize(request) == false) {

            Bzs.TransactionResponse response =
                    Bzs.TransactionResponse.newBuilder().setStatus(Bzs.TransactionStatus.ABORTED).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }
        synchronized (this) {
            sequenceNumber+=1;
            responseHandlerRegistry.addToRegistry(epochNumber, sequenceNumber, request, responseObserver);
        }
    }

    void resetEpoch() {
        // Increment Epoch number and reset sequence number.
        synchronized (this) {
            epochNumber+=1;
            sequenceNumber=0;
        }
        // Process transactions in the current epoch. Pass the requests gathered during the epoch to BFT Client.
        Map<Integer, Bzs.Transaction> transactions = responseHandlerRegistry.getTransactions(epochNumber - 1);
        Map<Integer, StreamObserver<Bzs.TransactionResponse>> responseObservers =
                responseHandlerRegistry.getTransactionObservers(epochNumber - 1);
        BFTClient bftClient = new BFTClient(id);
        //TODO: Change the return typ to boolean
        List<Long> success = bftClient.performCommit(transactions.values());
        if (success==null) {

        }

    }

    public void setId(Integer id) {
        this.id=id;
    }
}
