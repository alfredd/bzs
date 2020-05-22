package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;
import io.grpc.stub.StreamObserver;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClusterDRWTProcessorImpl implements ClusterDRWTProcessor {
    private Bzs.TransactionBatch request;
    private StreamObserver<Bzs.TransactionBatchResponse> response;
    private final TxnProcessor processor;
    private String id;
    private Map<TransactionID, Bzs.Transaction> txnMap = new LinkedHashMap<>();
    private Bzs.TransactionBatchResponse.Builder batchResponseBuilder;
    private Integer preparedEpoch = -1;
    private static final Logger logger = Logger.getLogger(ClusterDRWTProcessorImpl.class.getName());
    private boolean commitCompleted = false;

    public void setPhase(Phase phase) {
        this.phase = phase;
    }

    private Phase phase = Phase.PREPARE;

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
            TransactionID transactionID = TransactionID.getTransactionID(transaction.getTransactionID());
            txnMap.put(transactionID, transaction);
        }
    }

    @Override
    public void setResponseObserver(StreamObserver<Bzs.TransactionBatchResponse> response) {
        this.response = response;
    }

    @Override
    public void addToFailedList(Bzs.Transaction t) {
//        TransactionID transactionID = TransactionID.getTransactionID(t.getTransactionID());
        logger.log(Level.WARNING, "Transaction is not serializable : "+t);
        Bzs.TransactionResponse tr = Bzs.TransactionResponse.newBuilder()
                .setTransactionID(t.getTransactionID())
                .setEpochNumber(t.getEpochNumber())
                .setStatus(Bzs.TransactionStatus.FAILURE)
                .build();
//        txnMap.remove(transactionID);
        batchResponseBuilder = batchResponseBuilder.addResponses(tr);
    }

    @Override
    public void addToFailedList(Bzs.TransactionResponse t) {
//        TransactionID transactionID = TransactionID.getTransactionID(t.getTransactionID());
        logger.log(Level.WARNING, "Transaction is not serializable : "+t);
        Bzs.TransactionResponse tr = Bzs.TransactionResponse.newBuilder()
                .setTransactionID(t.getTransactionID())
                .setEpochNumber(t.getEpochNumber())
                .setStatus(Bzs.TransactionStatus.FAILURE)
                .build();
//        txnMap.remove(transactionID);
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

        Bzs.TransactionBatchResponse response = batchResponseBuilder.build();
        logger.info("Sending 2PC transaction response: "+ response.getID());
        getResponseObserver().onNext(response);
        if (phase.equals(Phase.PREPARE)) {
            getResponseObserver().onCompleted();
        } else {
            commitCompleted = true;
        }
    }

    @Override
    public void setPreparedEpoch(Integer epochNumber) {
        this.preparedEpoch = epochNumber;
    }

    @Override
    public int getPreparedEpoch() {
        return preparedEpoch;
    }

    @Override
    public boolean commitCompleted() {
        return commitCompleted;
    }

}
