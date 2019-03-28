package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.bftcommit.BFTServer;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.exceptions.UnknownConfiguration;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public class BZStoreServer {

    private static final Logger logger = Logger.getLogger(BZStoreServer.class.getName());

    private int serverPort;

    private String id;

    private Server server;
    private TransactionProcessor transactionProcessor;

    public static void main(String[] args) throws IOException {
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        root.setLevel(ch.qos.logback.classic.Level.ERROR);
        if (args.length != 1) {
            System.err.println("Usage: ");
            System.err.println("      bzserver ID ; where ID=z{1..8}");
            System.exit(1);
        }

        String id = args[0];

        BZStoreProperties properties = new BZStoreProperties();
        try {
            Integer port = Integer.decode(
                    properties.getProperty(
                            id, BZStoreProperties.Configuration.port));
            logger.info(String.format("Server port configured at %d", port));
            BZStoreServer bzStoreServer = new BZStoreServer(id);
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

    public BZStoreServer(String id) {

        this.id = id;
        transactionProcessor = new TransactionProcessor();
        transactionProcessor.setId(Integer.decode(id));


    }

    private void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }

    private void start() throws IOException {
        server = ServerBuilder.forPort(this.serverPort)
                .addService(new BZStoreService(id, this.transactionProcessor))
                .addService(new BZStoreReplica(id, this.transactionProcessor))
                .build().start();
        logger.info("Server started.");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down.");
            BZStoreServer.this.stop();
        }));
        BFTServer bftServer = new BFTServer(Integer.decode(id));
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

