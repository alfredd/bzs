package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import io.grpc.stub.StreamObserver;

public interface ClusterDRWTProcessor {

    String getID();

    Bzs.TransactionBatch getRequest();

    StreamObserver<Bzs.TransactionBatchResponse> getResponse();

    void setRequest(Bzs.TransactionBatch request);

    void setResponse(StreamObserver<Bzs.TransactionBatchResponse> response);

    void addToFailedList(Bzs.Transaction t);
}
