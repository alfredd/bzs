package edu.ucsc.edgelab.db.bzs.configuration;

import edu.ucsc.edgelab.db.bzs.exceptions.UnknownConfiguration;

import java.io.IOException;

public class Configuration {

    private static final int DEFAULT_EPOCH_TIME_IN_MS = 50;

    public static ServerInfo getLeaderInfo() throws IOException, UnknownConfiguration {
        ServerInfo info = new ServerInfo();
        BZStoreProperties properties = new BZStoreProperties();

        String leaderID = properties.getProperty(BZStoreProperties.Configuration.leader);
        info.id = leaderID;
        info.host = properties.getProperty(leaderID, BZStoreProperties.Configuration.host);
        info.port = Integer.decode(properties.getProperty(leaderID, BZStoreProperties.Configuration.port));

        return info;
    }

    public static Integer getDefaultEpochTimeInMS() {
        return DEFAULT_EPOCH_TIME_IN_MS;
    }

}
