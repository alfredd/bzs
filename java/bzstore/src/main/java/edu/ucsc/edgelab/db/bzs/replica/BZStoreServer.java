package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.bftcommit.BFTServer;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.configuration.ServerInfo;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.exceptions.UnknownConfiguration;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public class BZStoreServer {

    private static final Logger logger = Logger.getLogger(BZStoreServer.class.getName());
    private Integer clusterID;

    private int serverPort;

    private Integer replicaID;

    private Server server;
    private TransactionProcessor transactionProcessor;

    public static void main(String[] args) throws IOException {
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        root.setLevel(ch.qos.logback.classic.Level.ERROR);
        if (args.length != 2) {
            System.err.println("Number of input arguments is not 2 but "+args.length);
            System.err.println("Usage: ");
            System.err.println("      bzserver CLUSTER_ID REPLICA_ID ; where ID={0..8}");
            System.exit(1);
        }

        Integer clusterID = Integer.decode(args[0]);
        Integer replicaID = Integer.decode(args[1]);

        BZStoreProperties properties = new BZStoreProperties();
        try {
            Integer port = Integer.decode(
                    properties.getProperty(
                            clusterID, replicaID, BZStoreProperties.Configuration.port));
            logger.info(String.format("Server port configured at %d", port));
            BZStoreServer bzStoreServer = new BZStoreServer(replicaID, clusterID);
            bzStoreServer.setServerPort(port);
            bzStoreServer.start();
            try {
                bzStoreServer.blockUntilShutdown();
            } catch (InterruptedException e) {
                logger.log(Level.SEVERE, e.getLocalizedMessage(), e);
            }
        } catch (UnknownConfiguration unknownConfiguration) {
            unknownConfiguration.printStackTrace();
        }
    }

    public BZStoreServer(Integer id, Integer clusterId) {

        this.replicaID = id;
        this.clusterID = clusterId;
        transactionProcessor = new TransactionProcessor(this.replicaID, this.clusterID);
        try {
            BZDatabaseController.initDB(clusterId,replicaID);
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getLocalizedMessage(),e);
        }
    }

    private void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }

    private void start() throws IOException {
        ServerInfo leaderInfo = ServerInfo.getLeaderInfo(clusterID);
        boolean isLeader = amITheLeader(leaderInfo);
        server = ServerBuilder.forPort(this.serverPort)
                .addService(new BZStoreService(replicaID, clusterID, this.transactionProcessor, isLeader))
                .addService(new BZStoreReplica(clusterID, replicaID, this.transactionProcessor, isLeader))
                .addService(new ClusterService(clusterID, replicaID, this.transactionProcessor, isLeader))
                .addService(new PkiService(clusterID))
                .build().start();
        if (isLeader)
            transactionProcessor.initTransactionProcessor();
        logger.info("Server started.");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down.");
            BZStoreServer.this.stop();
        }));
        BFTServer bftServer = new BFTServer(replicaID, isLeader);
    }

    private boolean amITheLeader(ServerInfo leaderInfo) {
        return leaderInfo.replicaID.equals(this.replicaID);
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            logger.info("Awaiting termination.");
            server.awaitTermination();
        }
    }
}

