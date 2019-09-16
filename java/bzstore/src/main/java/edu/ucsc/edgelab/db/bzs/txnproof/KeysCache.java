package edu.ucsc.edgelab.db.bzs.txnproof;

import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.configuration.ServerInfo;
import edu.ucsc.edgelab.db.bzs.replica.PkiServiceClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class KeysCache {

    public static final Logger logger = Logger.getLogger(KeysCache.class.getName());

    public static final Map<Integer, String> publicKeyMap = new ConcurrentHashMap<>();
    public static final Map<Integer, String> privateKeyMap = new ConcurrentHashMap<>();


    public static void initKeyCache(int cid) {
        int replicaCount = 4;
        for (int i = 0; i < replicaCount; i++) {
            try {
                ServerInfo replicaInfo = Configuration.getServerInfo(cid, i);
                PkiServiceClient pkiServiceClient = new PkiServiceClient(replicaInfo.host, replicaInfo.port);
                String replicaPublicKey = pkiServiceClient.getPublicKey();
                String replicaPrivateKey = pkiServiceClient.getPrivateKey();

                publicKeyMap.put(i, replicaPublicKey);
                privateKeyMap.put(i, replicaPrivateKey);
                pkiServiceClient.shutdown();
            } catch (Exception e) {
                logger.info("Could not get server info for replica: " + i + " from property file.");
            }
        }
    }

    public static String getPublicKey(int replicaID) {
        return getPKey(publicKeyMap, replicaID);
    }

    public static String getPrivateKey(int replicaID) {
        return getPKey(privateKeyMap, replicaID);
    }

    private static String getPKey(Map<Integer, String> map, int key) {
        if (map.containsKey(key))
            map.get(key);
        return null;
    }

}
