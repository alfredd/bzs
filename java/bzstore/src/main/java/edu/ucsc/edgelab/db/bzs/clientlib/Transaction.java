package edu.ucsc.edgelab.db.bzs.clientlib;

import edu.ucsc.edgelab.db.bzs.BZStoreClient;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.exceptions.CommitAbortedException;

import java.util.logging.Logger;

public class Transaction extends TransactionManager implements TransactionInterface {

    private BZStoreClient client;

    public static final Logger LOGGER = Logger.getLogger(Transaction.class.getName());

    public Transaction() {
        super();
    }

    public Transaction(String host, int port) {
        this();
        setClient(host, port);
    }

    public void setClient(String host, int port) {
        this.client = new BZStoreClient(host, port);
    }

    public void setClient(BZStoreClient client) {
        this.client = client;
    }

    @Override
    public String read(String key) {

        long startTime = System.currentTimeMillis();
        Bzs.Read read = Bzs.Read.newBuilder().setKey(key).build();
        Bzs.ReadResponse response = client.read(read);
        String responseKey = response.getKey();
        String responseValue = response.getValue();
        String digest = response.getResponseDigest();
        long responseVersion = response.getVersion();
        setReadHistory(responseKey, responseValue, responseVersion, digest);
        long duration = System.currentTimeMillis() - startTime;
        LOGGER.info("Read operation processed in "+duration+" msecs");
        return responseValue;
    }


    @Override
    public void commit() throws CommitAbortedException {
        long startTime = System.currentTimeMillis();
        LOGGER.info("Committing Transaction: "+ getTransaction().toString());
        Bzs.TransactionResponse response = client.commit(getTransaction());
        long duration = System.currentTimeMillis() - startTime;
        LOGGER.info("Transaction processed in "+duration+" msecs");
        LOGGER.info("Transaction Response: "+response);
        if(response.getStatus().equals(Bzs.TransactionStatus.ABORTED)) {
            LOGGER.info("Transaction was aborted.");
            throw new CommitAbortedException("Transaction was aborted"+response.toString());
        } else {
            if (response.getStatus().equals(Bzs.TransactionStatus.COMMITTED)) {
                LOGGER.info("Transaction committed.");
            }
        }
    }
}
