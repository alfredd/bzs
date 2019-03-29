package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.BZStoreGrpc;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.ForwardingClient;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.configuration.ServerInfo;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.exceptions.UnknownConfiguration;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

class BZStoreService extends BZStoreGrpc.BZStoreImplBase {

    private static final Logger log = Logger.getLogger(BZStoreService.class.getName());
    private final String id;
    private TransactionProcessor transactionProcessor;
    private ForwardingClient forwardingClient = null;

    public BZStoreService(String id, TransactionProcessor tp) {
        log.info("BZStore service started. Replica ID: " + id);
        this.id = id;

        transactionProcessor = tp;
        ServerInfo leaderInfo;
        try {
            leaderInfo = Configuration.getLeaderInfo();
        } catch (Exception e) {
            String msg = "Cannot create connection to leader.";
            log.log(Level.SEVERE, msg);
            throw new UnknownConfiguration(msg, e);
        }
        if (leaderInfo!=null && !this.id.equals(leaderInfo.id)) {
            forwardingClient = new ForwardingClient(leaderInfo.host, leaderInfo.port);
        } else if (this.id.equals(leaderInfo.id)) {
            tp.initTransactionProcessor();
        }
    }

    @Override
    public void commit(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {

        Bzs.TransactionResponse response;
        try {
            ServerInfo leader = Configuration.getLeaderInfo();
            if (leader.id.equals(id)) {
                // If this instance is the leader process the transaction.
                transactionProcessor.processTransaction(request, responseObserver);
            } else {
                response = forwardingClient.forward(request);
                responseObserver.onNext(response);
                responseObserver.onCompleted();
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
        String key = request.getKey();
        BZStoreData data = BZDatabaseController.getlatest(key);
        if (data.version>0) {

            Bzs.ReadResponse response = Bzs.ReadResponse.newBuilder()
                    .setKey(key)
                    .setResponseDigest(data.digest)
                    .setValue(data.value)
                    .setVersion(data.version)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } else {

            log.log(Level.WARNING, "Could not retrieve data for key: " + key);
            Bzs.ReadResponse response = Bzs.ReadResponse.newBuilder().setStatus(Bzs.OperationStatus.FAILED).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
