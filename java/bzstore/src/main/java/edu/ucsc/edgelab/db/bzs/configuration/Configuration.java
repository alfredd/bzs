package edu.ucsc.edgelab.db.bzs.configuration;

import edu.ucsc.edgelab.db.bzs.exceptions.UnknownConfiguration;

import java.io.IOException;

public class Configuration {

    public static final int WAIT_TIMEOUT = 300;
    private static final int DEFAULT_EPOCH_TIME_IN_MS = 50;

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

}
