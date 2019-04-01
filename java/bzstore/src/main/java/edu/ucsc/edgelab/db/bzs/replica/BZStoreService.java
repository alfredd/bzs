package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.BZStoreGrpc;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.ForwardingClient;
import edu.ucsc.edgelab.db.bzs.clientlib.Transaction;
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
    private final Integer replicaID;
    private final Integer clusterID;
    private TransactionProcessor transactionProcessor;
    private ForwardingClient forwardingClient = null;

    public BZStoreService(Integer id, Integer clusterID, TransactionProcessor tp, boolean isLeader) {
        log.info("BZStore service started. Replica ID: " + id);
        this.replicaID = id;
        this.clusterID = clusterID;

        transactionProcessor = tp;
        ServerInfo leaderInfo = ServerInfo.getLeaderInfo(clusterID);
        if (!isLeader)
            forwardingClient = new ForwardingClient(leaderInfo.host, leaderInfo.port);
    }

    @Override
    public void commit(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {

        Bzs.TransactionResponse response;
        try {
            ServerInfo leader = Configuration.getLeaderInfo(clusterID);
            if (leader.replicaID.equals(replicaID)) {
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
        Bzs.OperationStatus status = Bzs.OperationStatus.SUCCESS;
        ;
        String key = request.getKey();
        BZStoreData data;
        String[] keys = key.split(":");
        if (keys.length == 3 && !keys[0].equals(clusterID.toString())) {
            int cid = Integer.decode(keys[0]);
            int rid = Integer.decode(keys[1]);
            key = keys[2];
            // Get Key from specific node.
            ServerInfo remote;
            try {

                remote = ServerInfo.getReplicaInfo(cid, rid);
                Transaction t = new Transaction();
                t.setClient(remote.host, remote.port);
                data = t.read(key);
            } catch (Exception e) {
                log.severe(e.getLocalizedMessage());
                data = new BZStoreData();
                status = Bzs.OperationStatus.FAILED;
            }
        } else if (keys.length == 2) {
            log.log(Level.WARNING, "Could not retrieve data for key: " + key);
            data = new BZStoreData();
            status = Bzs.OperationStatus.INVALID;
        } else {
            data = BZDatabaseController.getlatest(key);
        }
        if (data.version <= 0 && status.equals(Bzs.OperationStatus.SUCCESS)) {
            status = Bzs.OperationStatus.INVALID;
        }

        Bzs.ReadResponse response = Bzs.ReadResponse.newBuilder()
                .setKey(clusterID + ":" + replicaID + ":" + key)
                .setResponseDigest(data.digest)
                .setValue(data.value)
                .setVersion(data.version)
                .setStatus(status)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();

    }
}
