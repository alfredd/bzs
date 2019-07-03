package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import io.grpc.stub.StreamObserver;

public interface ClusterDRWTProcessor {

    void clear();

    String getID();

    Bzs.TransactionBatch getRequest();

    StreamObserver<Bzs.TransactionBatchResponse> getResponseObserver();

    void setRequest(Bzs.TransactionBatch request);

    void setResponseObserver(StreamObserver<Bzs.TransactionBatchResponse> response);

    void addToFailedList(Bzs.Transaction t);

    void addToProcessedList(Bzs.TransactionResponse txnResponse);

    void sendResponseToClient();
}
