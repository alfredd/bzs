package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.ReplicaGrpc;
import io.grpc.stub.StreamObserver;

import java.util.logging.Logger;

public class BZStoreReplica extends ReplicaGrpc.ReplicaImplBase {

    private static final Logger log = Logger.getLogger(BZStoreReplica.class.getName());
    private final Integer id;
    private TransactionProcessor transactionProcessor;

    public BZStoreReplica(Integer id, Integer clusterID, TransactionProcessor transactionProcessor) {
        log.info("Forwarding service created. Replica replicaID: " + id);
        this.id = id;
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
