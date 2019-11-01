package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTServer;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterConnector;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.configuration.ServerInfo;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.exceptions.UnknownConfiguration;
import edu.ucsc.edgelab.db.bzs.txn.TxnProcessor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Arrays;
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
    private TxnProcessor transactionProcessor;

    public static void main(String[] args) throws IOException {
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        root.setLevel(ch.qos.logback.classic.Level.TRACE);
        if (args.length > 3) {
            printOptionsAndExit(args);
        }
        boolean runBenchmarks=false;
        Integer clusterID = Integer.decode(args[0]);
        Integer replicaID = Integer.decode(args[1]);
        logger.info("Input args: "+Arrays.toString(args));
        if (args.length<=3) {
            if ("y".equalsIgnoreCase(args[2].trim())) {
                runBenchmarks=true;
            } else {
                runBenchmarks = false;
            }
        } else {
            printOptionsAndExit(args);
        }
        ID.setRunBenchMarkTests(runBenchmarks);
        logger.info("Benchmarks will be run? "+ ID.canRunBenchMarkTests());
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

    private static void printOptionsAndExit(String[] args) {
        int i=0;
        for (String arg: args) {
            System.err.println(i+" "+arg);
            i++;
        }
        System.err.println("Invalid Usage. Please read the following:");
        System.err.println("  USAGE: ");
        System.err.println("      bzserver CLUSTER_ID REPLICA_ID [RUN_BENCHMARK];");
        System.err.println("            where ID={0..8}; optional and experimental RUN_BENCHMARK=y");
        System.exit(1);
    }

    public BZStoreServer(Integer id, Integer clusterId) {

        this.replicaID = id;
        this.clusterID = clusterId;
        ID.setIDs(clusterID,replicaID);
        try {
            BZDatabaseController.initDB(clusterId,replicaID);
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getLocalizedMessage(),e);
        }
        DependencyVectorManager.init();
        transactionProcessor = new TxnProcessor();
    }

    private void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }

    private void start() throws IOException {
        ServerInfo leaderInfo = ServerInfo.getLeaderInfo(clusterID);
        boolean isLeader = amITheLeader(leaderInfo);
//        if (isLeader)
//            transactionProcessor.initTransactionProcessor();
        server = ServerBuilder.forPort(this.serverPort)
                .addService(new BZStoreService(this.transactionProcessor, isLeader))
                .addService(new BZStoreReplica(this.transactionProcessor, isLeader))
                .addService(new ClusterService(this.transactionProcessor, isLeader))
                .addService(new PkiService())
                .build().start();
        logger.info("Server started.");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down.");
            BZStoreServer.this.stop();
        }));
        logger.info("Starting up BFTServer for "+ID.string());
        BFTServer bftServer = new BFTServer(isLeader);
        logger.info("Creating connection to BFT Server. ");
        BFTClient.createConnection();
        ClusterConnector.init();
    }

    private boolean amITheLeader(ServerInfo leaderInfo) {
        return leaderInfo.replicaID.equals(this.replicaID);
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
        BZDatabaseController.close();
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            logger.info("Awaiting termination.");
            server.awaitTermination();
        }
    }
}

