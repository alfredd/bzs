package edu.ucsc.edgelab.db.bzs.clientlib;

import edu.ucsc.edgelab.db.bzs.Bzs;

public class ConnectionLessTransaction {
    protected Bzs.Transaction transaction;
    protected Bzs.Transaction.Builder builder = Bzs.Transaction.newBuilder();

    public ConnectionLessTransaction() {
        this.transaction = builder.build();
        builder = Bzs.Transaction.newBuilder();
    }



    public void setReadHistory(String responseKey, String responseValue, long responseVersion,
                               Integer clusterId) {
        Bzs.ReadHistory history = Bzs.ReadHistory.newBuilder()
                .setReadOperation(Bzs.Read.newBuilder().setClusterID(clusterId).setKey(responseKey).build())
                .setValue(responseValue)
                .setVersion(responseVersion)
//                .setResponseDigest(digest)
                .build();
        transaction = builder.addReadHistory(history).build();

//        logTransaction();
    }

    public void logTransaction() {
        Transaction.LOGGER.info("Transaction object till now: "+transaction.toString());
    }

    @Deprecated
    public void write(String key, String value) {
        Bzs.Write write = Bzs.Write.newBuilder().setKey(key).setValue(value).build();
        transaction = builder.addWriteOperations(write).build();
//        logTransaction();
    }

    public void write(String key, String value, Integer clusterId) {
        Bzs.Write write = Bzs.Write.newBuilder().setKey(key).setValue(value).setClusterID(clusterId).build();
        transaction = builder.addWriteOperations(write).build();
//        logTransaction();
    }

    public Bzs.Transaction getTransaction() {
        return transaction;
    }
}
