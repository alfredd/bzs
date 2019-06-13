package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.TransactionCache;
import edu.ucsc.edgelab.db.bzs.replica.Serializer;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;
import io.grpc.stub.StreamObserver;

public class TransactionProcessor {

    private final Integer clusterID;
    private final Integer replicaID;
    private Serializer serializer;
    private EpochManager epochManager;

    public TransactionProcessor(Integer clusterID, Integer replicaID) {
        this.clusterID = clusterID;
        this.replicaID = replicaID;
        serializer = new Serializer(clusterID, replicaID);
        epochManager = new EpochManager();
    }

    public void processTransaction(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
        if (!serializer.serialize(request)) {

        }
        TransactionID tid = epochManager.getTID();
        TransactionCache.update(tid,request);
    }
}
