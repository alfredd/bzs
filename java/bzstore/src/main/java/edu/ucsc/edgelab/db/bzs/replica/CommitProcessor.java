package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.MessageType;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterClient;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterConnector;
import edu.ucsc.edgelab.db.bzs.txn.TxnUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Must refactor at some point.
 */
public class CommitProcessor extends RemoteOpProcessor {

    public static final Logger LOG = Logger.getLogger(CommitProcessor.class.getName());

    public CommitProcessor(Integer cid, Integer rid, TransactionID tid, Bzs.Transaction transaction,
                           ClusterConnector clusterConnector) {
        super(cid, rid, tid, transaction, clusterConnector);
    }

    @Override
    public void run() {
        Set<Integer> remoteCIDs = TxnUtils.getListOfClusterIDs(remoteTransaction, cid);
        List<Thread> remoteThreads = sendMessageToClusterLeaders(remoteCIDs, MessageType.Commit);

        joinAllThreads(remoteThreads);
        boolean prepared = true;
        LOG.info("Remote responses size: " + remoteResponses.size());
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
        responseObserver.commitOperationObserver(tid, transactionStatus);
    }

    @Override
    public List<Thread> sendMessageToClusterLeaders(Set<Integer> remoteCIDs, MessageType messageType) {

        List<Thread> remoteThreads = new LinkedList<>();
        for (int cid : remoteCIDs) {
            Thread t = new Thread(() -> {
                if (messageType == MessageType.Commit) {
                    Bzs.TransactionResponse response = clusterConnector.execute(ClusterClient.DRWT_Operations.COMMIT, remoteTransaction, cid);
                    remoteResponses.put(cid, response);
                }
            });
            t.start();
            remoteThreads.add(t);
        }
        return remoteThreads;
    }
}
