package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.BZStoreGrpc;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.ForwardingClient;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.configuration.ServerInfo;
import edu.ucsc.edgelab.db.bzs.exceptions.UnknownConfiguration;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

class BZStoreService extends BZStoreGrpc.BZStoreImplBase {

    private static final Logger log = Logger.getLogger(BZStoreService.class.getName());
    private final String id;
    private Configuration configuration;
    private TransactionProcessor transactionProcessor;
    private ForwardingClient forwardingClient =null;

    public BZStoreService(String id) {
        log.info("BZStore service started. Replica ID: " + id);
        this.id = id;
        configuration = new Configuration();
        transactionProcessor = new TransactionProcessor();
        ServerInfo leaderInfo = null;
        try {
            leaderInfo = configuration.getLeaderInfo();
        } catch (Exception e) {
            String msg = "Cannot create connection to leader.";
            log.log(Level.SEVERE, msg);
            throw new UnknownConfiguration(msg,e);
        }
        if (!this.id.equals(leaderInfo.id)) {
            forwardingClient = new ForwardingClient(leaderInfo.host,leaderInfo.port);
        }
    }

    @Override
    public void commit(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {

        Bzs.TransactionResponse response;
        try {
            ServerInfo leader = configuration.getLeaderInfo();
            if (leader.id.equals(id)) {
                // If this instance is the leader process the transaction.
                transactionProcessor.processTransaction(request,responseObserver);
            } else {
                // If this instance is not the leader forward transaction to the leader.
                forwardingClient.forward(request,responseObserver);
            }
        } catch (IOException e) {
            log.log(Level.SEVERE, "Could not connect to leader. Aborting transaction.", e);
            response = Bzs.TransactionResponse.newBuilder().setStatus(Bzs.TransactionStatus.ABORTED).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (UnknownConfiguration unknownConfiguration) {
            log.log(Level.SEVERE, "Could not connect to leader due to configuration error. " +
                    "Aborting transaction.", unknownConfiguration);
            response = Bzs.TransactionResponse.newBuilder().setStatus(Bzs.TransactionStatus.ABORTED).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

    }

    @Override
    public void rOCommit(Bzs.ROTransaction request, StreamObserver<Bzs.ROTransactionResponse> responseObserver) {
        super.rOCommit(request, responseObserver);
    }

    @Override
    public void readOperation(Bzs.Read request, StreamObserver<Bzs.ReadResponse> responseObserver) {
        super.readOperation(request, responseObserver);
    }
}
