package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import io.grpc.stub.StreamObserver;

public class RemoteTransactionProcessor  {

    public Integer clusterID;
    public Integer replicaID;

    public RemoteTransactionProcessor(Integer clusterID, Integer replicaID) {
        this.clusterID = clusterID;
        this.replicaID = replicaID;
    }

    public void processAsync (TransactionID tid,
                              Bzs.Transaction request,
                              StreamObserver<Bzs.TransactionResponse> responseObserver) {


    }
}
