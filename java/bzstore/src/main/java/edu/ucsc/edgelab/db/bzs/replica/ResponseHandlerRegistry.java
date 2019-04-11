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

    public void addToRegistry(final int epochNumber, final int sequenceNumber, Bzs.Transaction transaction,
                              StreamObserver<Bzs.TransactionResponse> responseObserver) {

        Map<Integer, StreamObserver<Bzs.TransactionResponse>> epochHistory =
                requestHandlerRegistry.computeIfAbsent(epochNumber, k -> new LinkedHashMap<>());
        epochHistory.put(sequenceNumber, responseObserver);

        Map<Integer, Bzs.Transaction> epochTransactionHistory = requestRegistry.computeIfAbsent(epochNumber,
                k -> new LinkedHashMap<>());
        epochTransactionHistory.put(sequenceNumber, transaction);

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

    public Bzs.Transaction getTransaction(int epochNumber, int sequenceNumber) {
        Map<Integer, Bzs.Transaction> transactions = getTransactions(epochNumber);
        if (transactions!=null)
            return transactions.get(sequenceNumber);
        return null;
    }

    public StreamObserver<Bzs.TransactionResponse> getStreamObserver(int epochNumber, int sequenceNumber) {
        Map<Integer, StreamObserver<Bzs.TransactionResponse>> streamObservers = getTransactionObservers(epochNumber);
        if (streamObservers!=null)
            return streamObservers.get(sequenceNumber);
        return null;
    }

}
