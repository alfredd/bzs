package edu.ucsc.edgelab.db.bzs.configuration;

import edu.ucsc.edgelab.db.bzs.exceptions.UnknownConfiguration;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Configuration {

    public static final int WAIT_TIMEOUT = 5000;
    private static final int DEFAULT_EPOCH_TIME_IN_MS = 50;
    public static final int MAX_EPOCH_TXN = 2000;
    public static final long MAX_EPOCH_DURATION_MS = 30;

    public static final Logger LOGGER = Logger.getLogger(Configuration.class.getName());

    public static ServerInfo getLeaderInfo(Integer clusterID) throws IOException, UnknownConfiguration {

        BZStoreProperties properties = new BZStoreProperties();
        Integer leaderID = Integer.decode(properties.getProperty(clusterID, BZStoreProperties.Configuration.leader));
        ServerInfo info = getServerInfo(clusterID, leaderID);

        return info;
    }

    public static ServerInfo getServerInfo(Integer clusterID, Integer leaderID) throws IOException {
        BZStoreProperties properties = new BZStoreProperties();
        ServerInfo info = new ServerInfo();
        info.clusterID = clusterID;
        info.replicaID = leaderID;
        info.host = properties.getProperty(clusterID, leaderID, BZStoreProperties.Configuration.host);
        info.port = Integer.decode(properties.getProperty(clusterID, leaderID, BZStoreProperties.Configuration.port));
        return info;
    }

    public static Integer getDefaultEpochTimeInMS() {
        return DEFAULT_EPOCH_TIME_IN_MS;
    }

    public static int clusterCount() {
        Integer clusterCount = 2;
        try {
            BZStoreProperties bzsProperties = new BZStoreProperties();
            clusterCount = Integer.valueOf(bzsProperties.getProperty(BZStoreProperties.Configuration.cluster_count));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return clusterCount;
    }

    public static Integer getEpochTimeInMS() {
        int epochTime;
        try {
            BZStoreProperties properties = new BZStoreProperties();
            epochTime = Integer.decode(properties.getProperty(BZStoreProperties.Configuration.epoch_time_ms));
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Exception occurred while getting epoch time. " + e.getLocalizedMessage());
            epochTime = edu.ucsc.edgelab.db.bzs.configuration.Configuration.getDefaultEpochTimeInMS();
        }
        return epochTime;
    }
}
