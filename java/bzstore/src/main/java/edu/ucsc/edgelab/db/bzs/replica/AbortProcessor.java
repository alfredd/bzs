package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.MessageType;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterConnector;

import java.util.List;
import java.util.Set;

public class AbortProcessor extends RemoteOpProcessor {
    public AbortProcessor(Integer cid, Integer rid, TransactionID tid, Bzs.Transaction transaction, ClusterConnector clusterConnector) {
        super(cid, rid, tid, transaction, clusterConnector);
    }

    @Override
    public void run() {
        Set<Integer> remoteCIDs = getListOfClusterIDs();
        List<Thread> abortThreads = super.sendMessageToClusterLeaders(remoteCIDs, MessageType.Abort);
        joinAllThreads(abortThreads);
        Bzs.TransactionStatus transactionStatus = Bzs.TransactionStatus.ABORTED;
        responseObserver.abortOperationObserver(tid, transactionStatus);
    }
}
