package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.ReplicaGrpc;
import edu.ucsc.edgelab.db.bzs.txn.TxnProcessor;
import io.grpc.stub.StreamObserver;

import java.util.logging.Logger;

public class BZStoreReplica extends ReplicaGrpc.ReplicaImplBase {

    private static final Logger log = Logger.getLogger(BZStoreReplica.class.getName());
    private final Integer replicaID;
    private final Integer clusterID;
    private TxnProcessor transactionProcessor;

    public BZStoreReplica(Integer clusterID, Integer replicaID, TxnProcessor transactionProcessor,
                          boolean isLeader) {
        log.info("Forwarding service created. Replica replicaID: " + replicaID);
        this.replicaID = replicaID;
        this.clusterID = clusterID;
        this.transactionProcessor = transactionProcessor;
    }

    @Override
    public void forward(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
        log.info("Received forwarded message from a replica. Adding transaction to processing queue.");
        transactionProcessor.processTransaction(request,responseObserver);
    }

    @Override
    public void forwardROT(Bzs.ROTransaction request, StreamObserver<Bzs.ROTransactionResponse> responseObserver) {
        super.forwardROT(request, responseObserver);
    }

}
