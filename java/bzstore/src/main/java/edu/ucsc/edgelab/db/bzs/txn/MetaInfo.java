package edu.ucsc.edgelab.db.bzs.txn;

public class MetaInfo {
    public boolean localRead, localWrite, remoteRead, remoteWrite;

    public MetaInfo() {
        localRead = localWrite = remoteWrite = remoteRead = false;
    }
}
