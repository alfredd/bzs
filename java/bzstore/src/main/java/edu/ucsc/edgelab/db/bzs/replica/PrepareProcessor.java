package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.MessageType;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterConnector;

import java.util.List;
import java.util.Map;
import java.util.Set;

class PrepareProcessor extends RemoteOpProcessor {


    public PrepareProcessor(TransactionID tid, Bzs.Transaction transaction, Integer cid, Integer rid,
                            ClusterConnector c) {
        super(cid, rid, tid, transaction, c);
    }

    @Override
    public void run() {
        Set<Integer> remoteCIDs = getListOfClusterIDs();
        List<Thread> remoteThreads = sendMessageToClusterLeaders(remoteCIDs, MessageType.Prepare);

        joinAllThreads(remoteThreads);
        boolean prepared = true;
        for (Map.Entry<Integer, Bzs.TransactionResponse> entrySet : remoteResponses.entrySet()) {
            prepared = prepared && entrySet.getValue().getStatus().equals(Bzs.TransactionStatus.PREPARED);
            if (!prepared)
                break;
        }

        Bzs.TransactionStatus transactionStatus = Bzs.TransactionStatus.PREPARED;
        if (!prepared) {
            List<Thread> abortThreads = sendMessageToClusterLeaders(remoteCIDs, MessageType.Abort);
            joinAllThreads(abortThreads);
            transactionStatus = Bzs.TransactionStatus.ABORTED;
        }
        responseObserver.remoteOperationObserver(tid, transactionStatus);
    }

}
