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
        long startTime = System.currentTimeMillis();
        Bzs.Read read = Bzs.Read.newBuilder().setKey(key).build();
        Bzs.ReadResponse response = client.read(read);
        Bzs.ReadHistory history = Bzs.ReadHistory.newBuilder().setKey(response.getKey()).setValue(response.getValue()).setVersion(response.getVersion()).build();
        transaction = builder.addReadHistory(history).build();
        long duration = System.currentTimeMillis() - startTime;
        LOGGER.info("Read operation processed in "+duration+" msecs");
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
        long startTime = System.currentTimeMillis();
        LOGGER.info("Committing Transaction: "+transaction.toString());
        Bzs.TransactionResponse response = client.commit(transaction);
        long duration = System.currentTimeMillis() - startTime;
        LOGGER.info("Transaction processed in "+duration+" msecs");
        LOGGER.info("Transaction Response: "+response);
        if(response.getStatus().equals(Bzs.TransactionStatus.ABORTED)) {
            LOGGER.info("Transaction was aborted.");
        } else {
            if (response.getStatus().equals(Bzs.TransactionStatus.COMMITTED)) {
                LOGGER.info("Transaction committed.");
            }
        }
        try {
            client.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
