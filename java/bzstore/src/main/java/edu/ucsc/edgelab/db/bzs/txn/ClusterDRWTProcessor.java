package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;
import io.grpc.stub.StreamObserver;

import java.util.LinkedHashMap;
import java.util.Map;

public class ClusterDRWTProcessor {
    private final Bzs.TransactionBatch request;
    private final StreamObserver<Bzs.TransactionBatchResponse> response;

    private Map<TransactionID, Bzs.Transaction> txnMap = new LinkedHashMap<>();

    public ClusterDRWTProcessor(Bzs.TransactionBatch request, StreamObserver<Bzs.TransactionBatchResponse> responseObserver) {
        this.request = request;
        this.response = responseObserver;
        for (Bzs.Transaction transaction : request.getTransactionsList()) {
            txnMap.put(TransactionID.getTransactionID(transaction.getTransactionID()), transaction);
        }
    }

    public void prepare() {

    }

    public void commit() {

    }
}
