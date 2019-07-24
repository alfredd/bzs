package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.MessageType;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterConnector;
import edu.ucsc.edgelab.db.bzs.txn.TxnUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

class PrepareProcessor extends RemoteOpProcessor {

    public static final Logger log = Logger.getLogger(PrepareProcessor.class.getName());

    public PrepareProcessor(TransactionID tid, Bzs.Transaction transaction, Integer cid, Integer rid,
                            ClusterConnector c) {
        super(cid, rid, tid, transaction, c);
    }

    @Override
    public void run() {
        Set<Integer> remoteCIDs = new TxnUtils().getListOfClusterIDs(remoteTransaction,cid);
        log.info("Sending prepare message for TID: "+tid+" to cluster: "+ remoteCIDs);
        List<Thread> remoteThreads = sendMessageToClusterLeaders(remoteCIDs, MessageType.Prepare);

        joinAllThreads(remoteThreads);

        Bzs.TransactionStatus transactionStatus = Bzs.TransactionStatus.PREPARED;
        Set<Integer> abortCIDs = new HashSet<>();
        for (Map.Entry<Integer, Bzs.TransactionResponse> entrySet : remoteResponses.entrySet()) {
            boolean prepared =  entrySet.getValue().getStatus().equals(Bzs.TransactionStatus.PREPARED);
            if (prepared) {
                abortCIDs.add(entrySet.getKey());
            }
            else {

                transactionStatus = Bzs.TransactionStatus.ABORTED;

            }
        }

        if (transactionStatus.equals(Bzs.TransactionStatus.ABORTED) && abortCIDs.size()>0) {
            List<Thread> abortThreads = sendMessageToClusterLeaders(abortCIDs, MessageType.Abort);
            joinAllThreads(abortThreads);
        }
        log.info("Transaction prepare completed with status : "+transactionStatus+ ", for  TID: "+tid);
        responseObserver.prepareOperationObserver(tid, transactionStatus);
    }

}
