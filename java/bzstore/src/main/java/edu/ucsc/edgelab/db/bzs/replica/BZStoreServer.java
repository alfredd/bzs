package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.exceptions.UnknownConfiguration;

import java.io.IOException;

/**
 * Hello world!
 */
public class BZStoreServer {
    public static void main(String[] args) throws IOException {
        BZStoreProperties properties = new BZStoreProperties();
        try {
            properties.getProperty(BZStoreProperties.Configuration.server_port);
        } catch (UnknownConfiguration unknownConfiguration) {
            unknownConfiguration.printStackTrace();
        }
    }
}

