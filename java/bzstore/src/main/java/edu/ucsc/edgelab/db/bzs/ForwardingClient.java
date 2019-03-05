package edu.ucsc.edgelab.db.bzs;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ForwardingClient {

    private String host;
    private int port;

    private final ManagedChannel channel;

    private final ReplicaGrpc.ReplicaBlockingStub blockingStub;

    private static final Logger log = Logger.getLogger(ForwardingClient.class.getName());

    public ForwardingClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
        this.host = host;
        this.port = port;
    }

    private ForwardingClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = ReplicaGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        log.log(Level.FINE, "Shutting down forwarding client instance.");
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public Bzs.TransactionResponse forward(Bzs.Transaction batch) {
        return blockingStub.forward(batch);
    }


    public Bzs.ROTransactionResponse forwardROTranaction (Bzs.ROTransaction roTransaction) {
        return blockingStub.forwardROT(roTransaction);
    }
}
