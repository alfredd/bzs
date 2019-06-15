package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.txn.MetaInfo;

public class LocalDataVerifier {

    private final Integer clusterID;

    public LocalDataVerifier(Integer clusterID) {
        this.clusterID = clusterID;
    }

    public MetaInfo getMetaInfo(Bzs.Transaction transaction) {
        MetaInfo metaInfo = new MetaInfo();

        for (Bzs.Write writeOps : transaction.getWriteOperationsList()) {
            Integer cid = writeOps.getClusterID();
            if (!metaInfo.localWrite) {
                metaInfo.localWrite = cid == clusterID;
            }
            if (!metaInfo.remoteWrite)
                metaInfo.remoteWrite = cid != clusterID;

            if (metaInfo.remoteWrite && metaInfo.localWrite)
                break;
        }
        for (Bzs.ReadHistory readHistory : transaction.getReadHistoryList()) {
            if (!metaInfo.localRead)
                metaInfo.localRead = readHistory.getClusterID() == clusterID;

            if (!metaInfo.remoteRead)
                metaInfo.remoteRead = readHistory.getClusterID() != clusterID;

            if (metaInfo.remoteRead && metaInfo.localRead)
                break;
        }
        return metaInfo;
    }
}
