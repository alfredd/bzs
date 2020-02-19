package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.performance.BatchMetricsManager;
import io.grpc.stub.StreamObserver;

public interface TransactionProcessorINTF {
    void processTransaction(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver);

    BatchMetricsManager getBatchMetricsManager();
}
