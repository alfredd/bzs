package edu.ucsc.edgelab.db.bzs.configuration;

import edu.ucsc.edgelab.db.bzs.exceptions.UnknownConfiguration;

public class ServerInfo {
    public Integer clusterID;
    public Integer replicaID;
    public String host;
    public int port;

    public static ServerInfo getLeaderInfo(Integer clusterID) {
        ServerInfo leaderInfo;
        try {
            leaderInfo = Configuration.getLeaderInfo(clusterID);
        } catch (Exception e) {
            String msg = "Cannot get leader info. " + e.getLocalizedMessage();
            throw new UnknownConfiguration(msg, e);
        }
        return leaderInfo;
    }

    public static ServerInfo getReplicaInfo(Integer clusterID, Integer replicaID) {
        ServerInfo serverInfo;
        try {
            serverInfo = Configuration.getServerInfo(clusterID,replicaID);
        } catch (Exception e) {
            String msg = "Cannot get server info. " + e.getLocalizedMessage();
            throw new UnknownConfiguration(msg, e);
        }
        return serverInfo;
    }


}
