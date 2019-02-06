package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.exceptions.UnknownConfiguration;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Hello world!
 */
public class BZStoreServer {

    private static final Logger logger = Logger.getLogger(BZStoreServer.class.getName());

    public static void main(String[] args) throws IOException {
        BZStoreProperties properties = new BZStoreProperties();
        try {
            Integer serverPort = Integer.decode(properties.getProperty(BZStoreProperties.Configuration.server_port));
            logger.info(String.format("Server port configured at %d", serverPort));
        } catch (UnknownConfiguration unknownConfiguration) {
            unknownConfiguration.printStackTrace();
        }
    }
}

