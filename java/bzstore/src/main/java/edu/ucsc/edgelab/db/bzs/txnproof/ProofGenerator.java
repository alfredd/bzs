package edu.ucsc.edgelab.db.bzs.txnproof;

import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.configuration.ServerInfo;
import edu.ucsc.edgelab.db.bzs.replica.ID;
import edu.ucsc.edgelab.db.bzs.replica.PkiServiceClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class ProofGenerator {

    public static final Logger logger = Logger.getLogger(ProofGenerator.class.getName());

    public static final Map<Integer, PkiServiceClient> pkiMap = new ConcurrentHashMap<>();

    public static void init() {
        int replicaCount = 4;
        int cid = ID.getClusterID();
        for (int i = 0; i < replicaCount; i++) {
            try {
                ServerInfo replicaInfo = Configuration.getServerInfo(cid, i);
                PkiServiceClient pkiServiceClient = new PkiServiceClient(replicaInfo.host, replicaInfo.port);
                pkiMap.put(i, pkiServiceClient);
            } catch (Exception e) {
                logger.info("Could not get server info for replica: "+ i+" from property file.");
            }
        }
    }



}
