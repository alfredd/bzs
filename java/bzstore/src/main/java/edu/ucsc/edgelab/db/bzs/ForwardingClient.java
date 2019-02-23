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

    public Bzs.TransactionBatchResponse forward(Bzs.TransactionBatch batch) {
        Bzs.TransactionBatchResponse response = blockingStub.forward(batch);
        return response;
    }

    public void forward(Bzs.Transaction request, StreamObserver<Bzs.TransactionResponse> responseObserver) {
        Bzs.TransactionBatch batchRequest = Bzs.TransactionBatch.newBuilder().addTransactions(request).build();
        Bzs.TransactionBatchResponse batchResponse = forward(batchRequest);
        List<Bzs.TransactionResponse> responsesList = batchResponse.getResponsesList();
        for (Bzs.TransactionResponse tr : responsesList) {
            responseObserver.onNext(tr);
        }
        responseObserver.onCompleted();
    }
}
