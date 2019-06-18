package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

public class TxnUtils {

    private static final Logger logger = Logger.getLogger(TxnUtils.class.getName());

    public static Set<Integer> getListOfClusterIDs(final Bzs.Transaction remoteTransaction, final Integer cid) {

        Set<Integer> cidSet = new HashSet<>();

        for (int i = 0; i < remoteTransaction.getReadHistoryCount(); i++) {
            int clusterID = remoteTransaction.getReadHistory(i).getClusterID();
            addToCidSet(cidSet, clusterID, cid);
        }
        for (int i = 0; i < remoteTransaction.getWriteOperationsCount(); i++) {
            int clusterID = remoteTransaction.getWriteOperations(i).getClusterID();
            addToCidSet(cidSet, clusterID, cid);
        }
        logger.info("Set of CIDs to which the request will be sent: " + cidSet);
        return cidSet;
    }

    private static void addToCidSet(Set<Integer> cidSet, int clusterID, int myCid) {
        if (clusterID !=myCid)
            cidSet.add(clusterID);
    }
}
