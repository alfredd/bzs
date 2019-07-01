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


    public ClusterDRWTProcessorImpl(TxnProcessor processor) {
        this.processor = processor;
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
        for (Bzs.Transaction transaction : request.getTransactionsList()) {
            txnMap.put(TransactionID.getTransactionID(transaction.getTransactionID()), transaction);
        }
    }

    @Override
    public void setResponse(StreamObserver<Bzs.TransactionBatchResponse> response) {
        this.response = response;
    }

    @Override
    public void addToFailedList(Bzs.Transaction t) {

    }

}
