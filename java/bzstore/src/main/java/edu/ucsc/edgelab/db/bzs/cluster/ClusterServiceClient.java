package edu.ucsc.edgelab.db.bzs.cluster;

import edu.ucsc.edgelab.db.bzs.BZStoreClient;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.ClusterGrpc;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClusterServiceClient {
    private String host;
    private int port;

    private static final Logger log = Logger.getLogger(BZStoreClient.class.getName());
    private final ManagedChannel channel;
    private final ClusterGrpc.ClusterBlockingStub blockingStub;

    public ClusterServiceClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
        this.host = host;
        this.port = port;
    }

    private ClusterServiceClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = ClusterGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public boolean isConnected() {
        return !channel.isTerminated();
    }

    public Bzs.TransactionResponse commit(Bzs.Transaction transaction) {
        log.log(Level.INFO, "Beginning transaction commit for : " + transaction.toString());

        Bzs.TransactionResponse response = blockingStub.withDeadlineAfter(Configuration.WAIT_TIMEOUT, TimeUnit.MILLISECONDS).commit(transaction);
        log.log(Level.INFO, "Transaction committed with status: " + response.getStatus().name());
        return response;
    }

    public Bzs.ReadResponse read(Bzs.Read readOperation) {
        log.log(Level.FINE, "Beginning read operation for : " + readOperation.toString());
        Bzs.ReadResponse response = blockingStub.withDeadlineAfter(Configuration.WAIT_TIMEOUT, TimeUnit.MILLISECONDS).readOperation(readOperation);
        log.log(Level.FINE, String.format("Read version %d of key: %s.", response.getVersion(), response.getReadOperation().getKey()));
        return response;
    }

    public Bzs.TransactionResponse abort(Bzs.Transaction transaction) {
        log.log(Level.FINE, "Beginning Abort for : " + transaction.toString());
        Bzs.TransactionResponse response = blockingStub.withDeadlineAfter(Configuration.WAIT_TIMEOUT, TimeUnit.MILLISECONDS).abort(transaction);
        log.log(Level.FINE, "Abort completed with status: " + response.getStatus().name());
        return response;
    }

    public Bzs.TransactionResponse prepare(Bzs.Transaction transaction) {
        log.log(Level.INFO, "Beginning prepare commit for : " + transaction.toString());
        Bzs.TransactionResponse response = blockingStub.withDeadlineAfter(Configuration.WAIT_TIMEOUT, TimeUnit.MILLISECONDS).commitPrepare(transaction);
        log.log(Level.INFO, "prepare completed with status: " + response.getStatus().name());
        return response;
    }
}
