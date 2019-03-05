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

        return null;
    }

    @Override
    public void write(String key, String value) {

    }

    @Override
    public void commit() {
        client.commit(transaction);
    }
}
