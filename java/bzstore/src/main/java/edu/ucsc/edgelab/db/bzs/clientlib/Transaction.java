package edu.ucsc.edgelab.db.bzs.clientlib;

import edu.ucsc.edgelab.db.bzs.BZClient;
import edu.ucsc.edgelab.db.bzs.Bzs;

import java.util.logging.Logger;

public class Transaction implements TransactionInterface {

    private final BZClient client;
    private Bzs.Transaction transaction;
    private Bzs.Transaction.Builder builder;

    public static final Logger LOGGER = Logger.getLogger(Transaction.class.getName());

    public Transaction(String host, int port) {
        this.client = new BZClient(host, port);

        builder = Bzs.Transaction.newBuilder();
        this.transaction = builder.build();
    }

    @Override
    public String read(String key) {

//        response = client.read(readOperation);
        Bzs.Read read = Bzs.Read.newBuilder().setKey(key).build();
        Bzs.ReadResponse response = client.read(read);
        Bzs.ReadHistory history = Bzs.ReadHistory.newBuilder().setKey(response.getKey()).setVersion(response.getVersion()).build();
        transaction = builder.addReadHistory(history).build();
        logTransaction();
        return response.getValue();
    }

    public void logTransaction() {
        LOGGER.info("Transaction object till now: "+transaction.toString());
    }

    @Override
    public void write(String key, String value) {
        Bzs.Write write = Bzs.Write.newBuilder().setKey(key).setValue(value).build();
        transaction = builder.addWriteOperations(write).build();
        logTransaction();
    }

    @Override
    public void commit() {
        LOGGER.info("Committing Transaction: "+transaction.toString());
        client.commit(transaction);
        LOGGER.info("Committed Transaction: "+transaction.toString());
        try {
            client.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
