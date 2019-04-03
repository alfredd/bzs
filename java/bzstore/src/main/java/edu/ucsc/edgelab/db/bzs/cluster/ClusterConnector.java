package edu.ucsc.edgelab.db.bzs.cluster;

import edu.ucsc.edgelab.db.bzs.BZStoreClient;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimerTask;

public class ClusterConnector extends TimerTask {
    Map<Integer, BZStoreClient> clients = new LinkedHashMap<>();
    @Override
    public void run() {
        try {
            BZStoreProperties properties = new BZStoreProperties();
            Integer clusterCount =
                    Integer.decode(
                            properties.getProperty(BZStoreProperties.Configuration.cluster_count)
                    );
            for (int i = 0; i < clusterCount; i++) {

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
