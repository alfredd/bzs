package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.MessageType;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterConnector;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Must refactor at some point.
 */
public class CommitProcessor extends RemoteOpProcessor {
    public CommitProcessor(Integer cid, Integer rid, TransactionID tid, Bzs.Transaction transaction,
                           ClusterConnector clusterConnector) {
        super(cid, rid, tid, transaction, clusterConnector);
    }

    @Override
    public void run() {
        Set<Integer> remoteCIDs = getListOfClusterIDs();
        List<Thread> remoteThreads = sendMessageToClusterLeaders(remoteCIDs, MessageType.Commit);

        joinAllThreads(remoteThreads);
        boolean prepared = true;
        for (Map.Entry<Integer, Bzs.TransactionResponse> entrySet : remoteResponses.entrySet()) {
            prepared = prepared && entrySet.getValue().getStatus().equals(Bzs.TransactionStatus.COMMITTED);
            if (!prepared)
                break;
        }

        Bzs.TransactionStatus transactionStatus = Bzs.TransactionStatus.COMMITTED;
        if (!prepared) {
            List<Thread> abortThreads = super.sendMessageToClusterLeaders(remoteCIDs, MessageType.Abort);
            joinAllThreads(abortThreads);
            transactionStatus = Bzs.TransactionStatus.ABORTED;
        }
        responseObserver.remoteOperationObserver(tid, transactionStatus);
    }

    @Override
    public List<Thread> sendMessageToClusterLeaders(Set<Integer> remoteCIDs, MessageType messageType) {

        List<Thread> remoteThreads = new LinkedList<>();
        for (int cid : remoteCIDs) {
            Thread t = new Thread(() -> {
                if (messageType == MessageType.Commit) {
                    Bzs.TransactionResponse response = clusterConnector.commit(remoteTransaction, cid);
                    remoteResponses.put(cid, response);
                }
            });
            t.start();
            remoteThreads.add(t);
        }
        return remoteThreads;
    }
}
