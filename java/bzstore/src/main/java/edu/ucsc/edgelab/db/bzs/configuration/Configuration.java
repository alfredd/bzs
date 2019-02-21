package edu.ucsc.edgelab.db.bzs.configuration;

import edu.ucsc.edgelab.db.bzs.exceptions.UnknownConfiguration;

import java.io.IOException;

public class Configuration {

    public ServerInfo getLeaderInfo() throws IOException, UnknownConfiguration {
        ServerInfo info = new ServerInfo();
        BZStoreProperties properties = new BZStoreProperties();

        String leaderID = properties.getProperty(BZStoreProperties.Configuration.leader);
        info.id = leaderID;
        info.host = properties.getProperty(leaderID, BZStoreProperties.Configuration.host);
        info.port = properties.getProperty(leaderID, BZStoreProperties.Configuration.port);

        return info;
    }

}
