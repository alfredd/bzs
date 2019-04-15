package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterClient;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterConnector;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class RemoteProcessor implements Runnable {

    private final Integer cid;
    private final Integer rid;
    private final ClusterClient clusterConnector;
    private TransactionID tid;
//    private Bzs.Transaction transaction;
    Map<Integer, Bzs.TransactionResponse> remoteResponses = new ConcurrentHashMap<>();
    private TransactionProcessor responseObserver;
    private String transactionID;
    private Bzs.Transaction remoteTransaction;


    public RemoteProcessor(TransactionID tid, Bzs.Transaction transaction, Integer cid, Integer rid, ClusterConnector c) {
        this.tid = tid;
//        this.transaction = transaction;
        this.cid = cid;
        this.rid = rid;
        this.clusterConnector = c.getClusterClient();
        transactionID = String.format("%d:%d:%d:%d",cid,rid,tid.getEpochNumber(),tid.getSequenceNumber());
        remoteTransaction = Bzs.Transaction.newBuilder(transaction).setTransactionID(transactionID).build();
    }

    public void setResponseObserver(TransactionProcessor processor) {
        this.responseObserver = processor;
    }

    enum MessageType {
        Prepare, Abort, Commit
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
            List<Thread> abortThreads = sendMessageToClusterLeaders(remoteCIDs,MessageType.Abort);
            joinAllThreads(abortThreads);
            transactionStatus = Bzs.TransactionStatus.ABORTED;
        }
        responseObserver.prepared(tid, transactionStatus);
    }

    public void joinAllThreads(List<Thread> remoteThreads) {
        for (Thread t : remoteThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public List<Thread> sendMessageToClusterLeaders(Set<Integer> remoteCIDs, MessageType messageType) {

        List<Thread> remoteThreads = new LinkedList<>();
        for (int cid : remoteCIDs) {
            Thread t = new Thread(() -> {
                if (messageType==MessageType.Prepare) {
                    Bzs.TransactionResponse response = clusterConnector.commitPrepare(remoteTransaction, cid);
                    remoteResponses.put(cid, response);
                } else if(messageType==MessageType.Abort) {
                    Bzs.TransactionResponse response = clusterConnector.abort(remoteTransaction, cid);
                    remoteResponses.put(cid, response);
                }
            });
            t.start();
            remoteThreads.add(t);
        }
        return remoteThreads;
    }

    private Set<Integer> getListOfClusterIDs() {

        Set<Integer> cidSet = new HashSet<>();

        for (int i =0;i<remoteTransaction.getReadHistoryCount();i++) {
            cidSet.add(remoteTransaction.getReadHistory(i).getClusterID());
        }
        for (int i =0;i<remoteTransaction.getWriteOperationsCount();i++) {
            cidSet.add(remoteTransaction.getWriteOperations(i).getClusterID());
        }
        return cidSet;
    }
}
