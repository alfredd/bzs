package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.ForwardingClient;
import edu.ucsc.edgelab.db.bzs.PKIServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PkiServiceClient {

    private String host;
    private int port;

    private final ManagedChannel channel;

    private final PKIServiceGrpc.PKIServiceBlockingStub blockingStub;

    private static final Logger log = Logger.getLogger(ForwardingClient.class.getName());

    public PkiServiceClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
        this.host = host;
        this.port = port;
    }

    private PkiServiceClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = PKIServiceGrpc.newBlockingStub(channel);
    }

    public boolean isConnected() {
        return !channel.isTerminated();
    }

    public void shutdown() throws InterruptedException {
        log.log(Level.FINE, "Shutting down forwarding client instance.");
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public String getPublicKey() {
        Bzs.KeyType request = Bzs.KeyType.newBuilder().build();
        Bzs.Key response = blockingStub.getPublicKey(request);
        return response.getKey();
    }

    public String getPrivateKey() {
        Bzs.KeyType request = Bzs.KeyType.newBuilder().build();
        Bzs.Key response = blockingStub.getPrivateKey(request);
        return response.getKey();
    }
}
