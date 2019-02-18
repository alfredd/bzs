package edu.ucsc.edgelab.db.bzs;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BZClient {

    private String host;
    private int port;

    private static final Logger log = Logger.getLogger(BZClient.class.getName());
    private final ManagedChannel channel;
    private final BZStoreGrpc.BZStoreBlockingStub blockingStub;

    public BZClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host,port).usePlaintext().build());
        this.host = host;
        this.port = port;
    }

    private BZClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = BZStoreGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public Bzs.TransactionResponse commitTransaction (Bzs.Transaction transaction) {
        log.log(Level.FINE, "Beginning transaction commit for : "+transaction.toString());

        Bzs.TransactionResponse response = blockingStub.commit(transaction);
        log.log(Level.FINE, "Transaction completed with status: "+ response.getStatus().name());
        return response;
    }

//    public Commit.ROTransactionResponse
}
