package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.ReplicaGrpc;
import io.grpc.stub.StreamObserver;

import java.util.logging.Logger;

public class BZStoreReplica extends ReplicaGrpc.ReplicaImplBase {

    private static final Logger log = Logger.getLogger(BZStoreReplica.class.getName());
    private final String id;

    public BZStoreReplica(String id) {
        log.info("Forwarding service created. Replica id: " + id);
        this.id = id;
    }

    @Override
    public void forward(Bzs.TransactionBatch request, StreamObserver<Bzs.TransactionBatchResponse> responseObserver) {
        super.forward(request, responseObserver);
    }
}
