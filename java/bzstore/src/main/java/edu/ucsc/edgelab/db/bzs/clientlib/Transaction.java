package edu.ucsc.edgelab.db.bzs.clientlib;

import edu.ucsc.edgelab.db.bzs.BZClient;
import edu.ucsc.edgelab.db.bzs.Bzs;

public class Transaction implements TransactionInterface {

    private final BZClient client;
    private final Bzs.Transaction transaction;

    public Transaction(String host, int port) {
        this.client = new BZClient(host, port);
        this.transaction = Bzs.Transaction.newBuilder().build();
    }

    @Override
    public String read(String key) {

//        response = client.read(readOperation);
        Bzs.Read read = Bzs.Read.newBuilder().setKey(key).build();
        Bzs.ReadResponse response = client.read(read);
        Bzs.ReadHistory history = Bzs.ReadHistory.newBuilder().setKey(response.getKey()).setVersion(response.getVersion()).build();
        transaction.toBuilder().addReadHistory(history).build();
        return response.getValue();
    }

    @Override
    public void write(String key, String value) {
        Bzs.Write write = Bzs.Write.newBuilder().setKey(key).setValue(value).build();
        transaction.toBuilder().addWriteOperations(write).build();
    }

    @Override
    public void commit() {
        client.commit(transaction);
    }
}
