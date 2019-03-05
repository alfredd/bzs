package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import io.grpc.stub.StreamObserver;

import java.util.LinkedHashMap;
import java.util.Map;

public class ResponseHandlerRegistry {
    private Map<Integer, Map<Integer, StreamObserver<Bzs.TransactionResponse>>> requestHandlerRegistry;
    private Map<Integer, Map<Integer, Bzs.Transaction>> requestRegistry;

    public ResponseHandlerRegistry() {
        requestHandlerRegistry = new LinkedHashMap<>();
        requestRegistry = new LinkedHashMap<>();
    }

    public void addToRegistry(int epochNumber, int sequenceNumber, Bzs.Transaction transaction,
                              StreamObserver<Bzs.TransactionResponse> responseObserver) {

        Map<Integer, StreamObserver<Bzs.TransactionResponse>> epochHistory = requestHandlerRegistry.get(epochNumber);
        if (epochHistory==null) {
            epochHistory = new LinkedHashMap<>();
            requestHandlerRegistry.put(epochNumber,epochHistory);
        }
        epochHistory.put(sequenceNumber,responseObserver);

        Map<Integer, Bzs.Transaction> epochTransactionHistory = requestRegistry.get(epochNumber);
        if (epochTransactionHistory==null) {
            epochTransactionHistory = new LinkedHashMap<>();
            requestRegistry.put(epochNumber,epochTransactionHistory);
        }
        epochTransactionHistory.put(sequenceNumber,transaction);

    }


    public void clearEpochHistory(int epochNumber) {
        requestRegistry.remove(epochNumber);
        requestHandlerRegistry.remove(epochNumber);
    }

    public Map<Integer, Bzs.Transaction> getTransactions(int epochNumber) {
        return requestRegistry.get(epochNumber);
    }

    public Map<Integer, StreamObserver<Bzs.TransactionResponse>> getTransactionObservers(int epochNumber) {
        return requestHandlerRegistry.get(epochNumber);
    }

}
