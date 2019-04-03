package edu.ucsc.edgelab.db.bzs.cluster;

import edu.ucsc.edgelab.db.bzs.BZStoreClient;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClusterConnector extends TimerTask {
    private final Integer clusterID;
    private Map<Integer, BZStoreClient> clients;

    public static final Logger LOGGER = Logger.getLogger(ClusterConnector.class.getName());

    public ClusterConnector(Integer clusterID) {
        this.clusterID = clusterID;
        clients = new LinkedHashMap<>();
    }

    @Override
    public void run() {
        Integer clusterCount=0;
        BZStoreProperties properties=null;
        try {
            properties = new BZStoreProperties();
            clusterCount = Integer.decode(
                    properties.getProperty(BZStoreProperties.Configuration.cluster_count)
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (int i = 0; i < clusterCount; i++) {
            if (i == clusterID)
                continue;

            if (!clients.containsKey(i)) {
                createClient(properties, i);
            }
            BZStoreClient bzStoreClient = clients.get(i);
            if (!bzStoreClient.isConnected()) {
                try {
                    bzStoreClient.shutdown();
                } catch (InterruptedException e) {
                    LOGGER.log(Level.WARNING, "Exception occurred when trying to shutdown a client."+e.getLocalizedMessage());
                }
                createClient(properties, i);
            }
        }
    }

    public void createClient(BZStoreProperties properties, int i) {
        if (properties== null) {
            try {
                properties = new BZStoreProperties();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, e.getLocalizedMessage(),e);
                return;
            }
        }
        String replicaID = properties.getProperty(i, BZStoreProperties.Configuration.leader);
        String host = properties.getProperty(i, Integer.decode(replicaID), BZStoreProperties.Configuration.host);
        String port = properties.getProperty(i, Integer.decode(replicaID), BZStoreProperties.Configuration.port);
        int portInt = Integer.decode(port);
        clients.put(i, new BZStoreClient(host, portInt));
    }
}
