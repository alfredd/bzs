package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.ReplicaGrpc;
import edu.ucsc.edgelab.db.bzs.txn.TxnProcessor;
import io.grpc.stub.StreamObserver;

import java.util.logging.Logger;

public class BZStoreReplica extends ReplicaGrpc.ReplicaImplBase {

    private static final Logger log = Logger.getLogger(BZStoreReplica.class.getName());
    private TxnProcessor transactionProcessor;

    public BZStoreReplica(TxnProcessor transactionProcessor,
                          boolean isLeader) {
        log.info("Forwarding service created. Node: " + ID.string());
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
