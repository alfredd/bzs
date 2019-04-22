package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.cluster.ClusterConnector;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClusterKeysAccessor extends TimerTask {

    private Integer clusterID;
    private Integer MAX_CLUSTER_MEMBER = 4;

    private Map<Integer, PkiServiceClient> clients;
    public static final Logger LOGGER = Logger.getLogger(ClusterConnector.class.getName());

    public ClusterKeysAccessor(Integer clusterID) {
        this.clusterID = clusterID;
        clients = new LinkedHashMap<>();
    }


    @Override
    public void run() {
        Integer clusterCount = 0;
        BZStoreProperties properties = null;
        try {
            properties = new BZStoreProperties();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, e.getLocalizedMessage(), e);
        }
        for (int i = 0; i < MAX_CLUSTER_MEMBER; i++) {

            if (!clients.containsKey(i)) {
                createClient(properties, i);
            }
            PkiServiceClient pkiServiceClient = clients.get(i);
            if (!pkiServiceClient.isConnected()) {
                try {
                    pkiServiceClient.shutdown();
                } catch (InterruptedException e) {
                    LOGGER.log(Level.WARNING,
                            "Exception occurred when trying to shutdown a client." + e.getLocalizedMessage());
                }
                createClient(properties, i);
            }
        }
    }

    protected void createClient(BZStoreProperties properties, int i) {
        String host = properties.getProperty(clusterID, i, BZStoreProperties.Configuration.host);
        String port = properties.getProperty(clusterID, i, BZStoreProperties.Configuration.port);
        int portInt = Integer.decode(port);
        clients.put(i, new PkiServiceClient(host, portInt));
    }

    public List<String> getPublicKeys() {
        List<String> publicKeys = new LinkedList<>();
        for (int i = 0; i < MAX_CLUSTER_MEMBER; i++) {
            PkiServiceClient pkiServiceClient = clients.get(i);
            if (pkiServiceClient != null)
                publicKeys.add(pkiServiceClient.getPublicKey());
            else
                publicKeys.add("");
        }
        return publicKeys;
    }

    public List<String> getPrivateKeys() {
        List<String> privateKeys = new LinkedList<>();
        for (int i = 0; i < MAX_CLUSTER_MEMBER; i++) {
            PkiServiceClient pkiServiceClient = clients.get(i);
            if (pkiServiceClient != null)
                privateKeys.add(pkiServiceClient.getPrivateKey());
            else
                privateKeys.add("");
        }
        return privateKeys;
    }

}
