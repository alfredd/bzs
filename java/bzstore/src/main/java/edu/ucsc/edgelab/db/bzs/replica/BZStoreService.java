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
import edu.ucsc.edgelab.db.bzs.txn.TxnProcessor;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

class BZStoreService extends BZStoreGrpc.BZStoreImplBase {

    private static final Logger log = Logger.getLogger(BZStoreService.class.getName());
    private final Integer replicaID;
    private final Integer clusterID;
    private TxnProcessor transactionProcessor;
    private ForwardingClient forwardingClient = null;

    public BZStoreService(TxnProcessor tp, boolean isLeader) {
        this.replicaID = ID.getReplicaID();
        this.clusterID = ID.getClusterID();
        log.info("BZStore service started. Replica ID: " + replicaID);

        transactionProcessor = tp;
        ServerInfo leaderInfo = ServerInfo.getLeaderInfo(this.clusterID);
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
    public void rORead(Bzs.ROTransaction request, StreamObserver<Bzs.ROTransactionResponse> responseObserver) {

        log.info("Received ROT read request: " + request);
        Bzs.ROTransactionResponse.Builder responseBuilder = Bzs.ROTransactionResponse.newBuilder();
        for (Bzs.Read readRequest : request.getReadOperationsList()) {
            Bzs.ReadResponse readResponse = getReadResponse(readRequest);
            responseBuilder = responseBuilder.addReadResponses(readResponse);
        }
        Bzs.ROTransactionResponse response = responseBuilder.build();
        log.info("ROT response: " + response);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void rOSecondRead(Bzs.ROTransactionResponse request, StreamObserver<Bzs.ROTransactionResponse> responseObserver) {
        super.rOSecondRead(request, responseObserver);
    }

    @Override
    public void readOperation(Bzs.Read request, StreamObserver<Bzs.ReadResponse> responseObserver) {
        Bzs.ReadResponse response = getReadResponse(request);

//        log.info("Read response to client: "+response.toString());
        responseObserver.onNext(response);
        responseObserver.onCompleted();

    }

    private Bzs.ReadResponse getReadResponse(Bzs.Read request) {
        Bzs.OperationStatus status = Bzs.OperationStatus.SUCCESS;
        String key = request.getKey();
        Integer cid = request.getClusterID();
        Integer rid = request.getReplicaID();
        BZStoreData data;
        Bzs.ReadResponse.Builder responseBuilder = Bzs.ReadResponse.newBuilder();
//        log.info("Received read reqeust: "+request.toString());
        if (cid != clusterID) {
//            log.info("Read request is from another cluster ("+cid+")");
            // Get Key from specific node.
            ServerInfo remote;
            try {
                remote = ServerInfo.getReplicaInfo(cid, rid);
                Transaction readClient = new Transaction();
                readClient.setClient(remote.host, remote.port);
                data = readClient.read(request);
                readClient.close();
            } catch (Exception e) {
                log.log(Level.SEVERE, e.getLocalizedMessage(), e);
                data = new BZStoreData();
                status = Bzs.OperationStatus.FAILED;
            }
        } else {
            data = BZDatabaseController.getlatest(key);
            if (data.version != 0) {
                Bzs.SmrLogEntry smrBlock = BZDatabaseController.getSmrBlock(data.version);
                responseBuilder = responseBuilder.putAllDepVector(smrBlock.getDepVectorMap());
                responseBuilder = responseBuilder.setLce(smrBlock.getLce());
            }
        }

//        log.info("Read response from cluster "+cid+": "+data);
        return responseBuilder
                .setReadOperation(request)
                .setValue(data.value)
                .setVersion(data.version)
                .setStatus(status)
                .build();
    }
}
