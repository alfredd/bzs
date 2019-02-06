package edu.ucsc.edgelab.db.bzs.replica;

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
    private Server server;


    public static void main(String[] args) throws IOException {
        BZStoreProperties properties = new BZStoreProperties();
        try {
            Integer serverPort = Integer.decode(properties.getProperty(BZStoreProperties.Configuration.server_port));
            logger.info(String.format("Server port configured at %d", serverPort));
            BZStoreServer bzStoreServer = new BZStoreServer();
            bzStoreServer.setServerPort(serverPort);
            bzStoreServer.start();
            try {
                bzStoreServer.blockUntilShutdown();
            } catch (InterruptedException e) {
                logger.log(Level.SEVERE, e.getLocalizedMessage(),e);
            }
        } catch (UnknownConfiguration unknownConfiguration) {
            unknownConfiguration.printStackTrace();
        }
    }

    private void setServerPort(int serverPort) {
        this.serverPort=serverPort;
    }

    private void start() throws IOException {
        server = ServerBuilder.forPort(this.serverPort).addService(new BZStoreService()).build().start();
        logger.info("Server started.");
        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            logger.info("Shutting down.");
            BZStoreServer.this.stop();
        }));
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

