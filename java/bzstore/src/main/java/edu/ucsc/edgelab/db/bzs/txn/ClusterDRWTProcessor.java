package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import io.grpc.stub.StreamObserver;

import java.util.Map;

public interface ClusterDRWTProcessor {

    void clear();

    String getID();

    Bzs.TransactionBatch getRequest();

    StreamObserver<Bzs.TransactionBatchResponse> getResponseObserver();

    void setRequest(Bzs.TransactionBatch request);

    void setResponseObserver(StreamObserver<Bzs.TransactionBatchResponse> response);

    void addToFailedList(Bzs.Transaction t);

    void addToFailedList(Bzs.TransactionResponse t);

    void setDepVector(Map<Integer, Integer> depVector);

    void addProcessedResponse(Bzs.TransactionResponse txnResponse);

    void sendResponseToClient();

    void setPreparedEpoch(Integer epochNumber);

    int getPreparedEpoch();

    boolean commitCompleted();

    static enum Phase {
        PREPARE,
        COMMIT
    }
}
