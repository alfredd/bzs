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
    private Integer preparedEpoch = -1;


    public ClusterDRWTProcessorImpl(TxnProcessor processor) {
        this.processor = processor;
        clear();
    }

    public void prepare() {
        processor.prepareTransactionBatch(this);
    }

    public void commit() {
        processor.commitTransactionBatch(this);
    }

    @Override
    public void clear() {
        batchResponseBuilder = Bzs.TransactionBatchResponse.newBuilder();
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
    public StreamObserver<Bzs.TransactionBatchResponse> getResponseObserver() {
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
        batchResponseBuilder = batchResponseBuilder.addResponses(tr);
    }

    @Override
    public void setDepVector(Map<Integer, Integer> depVector) {
        batchResponseBuilder = batchResponseBuilder.putAllDepVector(depVector);
    }

    @Override
    public void addProcessedResponse(Bzs.TransactionResponse txnResponse) {
        batchResponseBuilder = batchResponseBuilder.addResponses(txnResponse);
    }

    @Override
    public void sendResponseToClient() {
        getResponseObserver().onNext(batchResponseBuilder.build());
        getResponseObserver().onCompleted();
    }

    @Override
    public void setPreparedEpoch(Integer epochNumber) {
        this.preparedEpoch = epochNumber;
    }

    @Override
    public int getPreparedEpoch() {
        return preparedEpoch;
    }

}
