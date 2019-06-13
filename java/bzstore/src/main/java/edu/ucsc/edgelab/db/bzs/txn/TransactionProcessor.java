package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.TransactionCache;
import edu.ucsc.edgelab.db.bzs.replica.Serializer;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;
import io.grpc.stub.StreamObserver;

import java.util.logging.Level;
import java.util.logging.Logger;

public class TransactionProcessor {

    private final Integer clusterID;
    private final Integer replicaID;
    private Serializer serializer;
    private EpochManager epochManager;
    public static final Logger log = Logger.getLogger(TransactionProcessor.class.getName());

    public TransactionProcessor(Integer clusterID, Integer replicaID) {
        this.clusterID = clusterID;
        this.replicaID = replicaID;
        serializer = new Serializer(clusterID, replicaID);
        epochManager = new EpochManager();
    }

    public void processTransaction(final Bzs.Transaction request, final StreamObserver<Bzs.TransactionResponse> responseObserver) {
        synchronized (this) {

            if (!serializer.serialize(request)) {
                log.info("Transaction cannot be serialized. Will abort. Request: " + request);
                Bzs.TransactionResponse response =
                        Bzs.TransactionResponse.newBuilder().setStatus(Bzs.TransactionStatus.ABORTED).build();
                if (responseObserver != null) {
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                } else {
                    log.log(Level.WARNING, "Transaction aborted: " + request.toString());
                }
                return;
            }
            TransactionID tid = epochManager.getTID();
            TransactionCache.add(tid,request,responseObserver);
        }
    }
}
