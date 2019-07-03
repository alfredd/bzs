package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;
import io.grpc.stub.StreamObserver;

import java.util.LinkedHashMap;
import java.util.Map;

public class ClusterDRWTProcessorImpl implements ClusterDRWTProcessor {
    private Bzs.TransactionBatch request;
    private StreamObserver<Bzs.TransactionBatchResponse> response;
    private final TxnProcessor processor;
    private String id;
    private Map<TransactionID, Bzs.Transaction> txnMap = new LinkedHashMap<>();
    private Bzs.TransactionBatchResponse.Builder batchResponseBuilder;


    public ClusterDRWTProcessorImpl(TxnProcessor processor) {
        this.processor = processor;
        batchResponseBuilder = Bzs.TransactionBatchResponse.newBuilder();
    }

    public void prepare() {
        processor.prepareTransactionBatch(this);
    }

    public void commit() {
        processor.commitTransactionBatch( this);
    }

    @Override
    public String getID() {
        return id;
    }

    @Override
    public Bzs.TransactionBatch getRequest() {
        return request;
    }

    @Override
    public StreamObserver<Bzs.TransactionBatchResponse> getResponse() {
        return response;
    }

    @Override
    public void setRequest(Bzs.TransactionBatch request) {
        this.request = request;
        id = request.getID();
        batchResponseBuilder = batchResponseBuilder.setID(id);
        for (Bzs.Transaction transaction : request.getTransactionsList()) {
            txnMap.put(TransactionID.getTransactionID(transaction.getTransactionID()), transaction);
        }
    }

    @Override
    public void setResponseObserver(StreamObserver<Bzs.TransactionBatchResponse> response) {
        this.response = response;
    }

    @Override
    public void addToFailedList(Bzs.Transaction t) {
        Bzs.TransactionResponse tr = Bzs.TransactionResponse.newBuilder()
                .setTransactionID(t.getTransactionID())
                .setEpochNumber(t.getEpochNumber())
                .setStatus(Bzs.TransactionStatus.FAILURE)
                .build();
        batchResponseBuilder.addResponses(tr);
    }

}
