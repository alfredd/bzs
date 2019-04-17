package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import io.grpc.stub.StreamObserver;

import java.util.LinkedHashMap;
import java.util.Map;

public class ResponseHandlerRegistry {
    private Map<Integer, Map<Integer, StreamObserver<Bzs.TransactionResponse>>> requestHandlerRegistry;
    private Map<Integer, Map<Integer, Bzs.Transaction>> requestRegistry;

    private Map<Integer, Map<Integer, StreamObserver<Bzs.TransactionResponse>>> remoteRequestHandlerRegistry;
    private Map<Integer, Map<Integer, Bzs.Transaction>> remoteRequestRegistry;

    public ResponseHandlerRegistry() {
        requestHandlerRegistry = new LinkedHashMap<>();
        requestRegistry = new LinkedHashMap<>();
        remoteRequestHandlerRegistry = new LinkedHashMap<>();
        remoteRequestRegistry = new LinkedHashMap<>();
    }


    public void addToRegistry(final TransactionID tid, Bzs.Transaction transaction,
                              StreamObserver<Bzs.TransactionResponse> responseObserver) {

    }

    @Deprecated
    public void addToRegistry(final int epochNumber, final int sequenceNumber, Bzs.Transaction transaction,
                              StreamObserver<Bzs.TransactionResponse> responseObserver) {

        Map<Integer, StreamObserver<Bzs.TransactionResponse>> epochHistory =
                requestHandlerRegistry.computeIfAbsent(epochNumber, k -> new LinkedHashMap<>());
        epochHistory.put(sequenceNumber, responseObserver);

        Map<Integer, Bzs.Transaction> epochTransactionHistory = requestRegistry.computeIfAbsent(epochNumber,
                k -> new LinkedHashMap<>());
        epochTransactionHistory.put(sequenceNumber, transaction);

    }


    public void clearLocalHistory(int epochNumber) {
        requestRegistry.remove(epochNumber);
        requestHandlerRegistry.remove(epochNumber);
    }
    public void clearRemoteHistory(int epochNumber) {
        remoteRequestRegistry.remove(epochNumber);
        remoteRequestHandlerRegistry.remove(epochNumber);
    }

    public Map<Integer, Bzs.Transaction> getLocalTransactions(int epochNumber) {

        return requestRegistry.get(epochNumber);
    }

    public Map<Integer, StreamObserver<Bzs.TransactionResponse>> getLocalTransactionObservers(int epochNumber) {
        return requestHandlerRegistry.get(epochNumber);
    }
    public Map<Integer, Bzs.Transaction> getRemoteTransactions(int epochNumber) {

        return remoteRequestRegistry.get(epochNumber);
    }

    public Map<Integer, StreamObserver<Bzs.TransactionResponse>> getRemoteTransactionObservers(int epochNumber) {
        return remoteRequestHandlerRegistry.get(epochNumber);
    }

    public Bzs.Transaction getTransaction(int epochNumber, int sequenceNumber) {
        Map<Integer, Bzs.Transaction> transactions = getLocalTransactions(epochNumber);
        if (transactions!=null)
            return transactions.get(sequenceNumber);
        return null;
    }

    public StreamObserver<Bzs.TransactionResponse> getStreamObserver(int epochNumber, int sequenceNumber) {
        Map<Integer, StreamObserver<Bzs.TransactionResponse>> streamObservers = getLocalTransactionObservers(epochNumber);
        if (streamObservers!=null)
            return streamObservers.get(sequenceNumber);
        return null;
    }

    public void addToRemoteRegistry(TransactionID tid, Bzs.Transaction request,
                                    StreamObserver<Bzs.TransactionResponse> responseObserver) {
        Integer epochNumber = tid.getEpochNumber();
        Integer sequenceNumber = tid.getSequenceNumber();

        Map<Integer, StreamObserver<Bzs.TransactionResponse>> epochHistory =
                remoteRequestHandlerRegistry.computeIfAbsent(epochNumber, k -> new LinkedHashMap<>());
        epochHistory.put(sequenceNumber, responseObserver);

        Map<Integer, Bzs.Transaction> epochTransactionHistory = remoteRequestRegistry.computeIfAbsent(epochNumber,
                k -> new LinkedHashMap<>());
        epochTransactionHistory.put(sequenceNumber, request);
    }
}
