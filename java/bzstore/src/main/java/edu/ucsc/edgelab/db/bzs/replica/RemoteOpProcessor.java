package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.MessageType;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterClient;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterConnector;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public abstract class RemoteOpProcessor implements Runnable {
    protected final Integer cid;
    protected final Integer rid;
    protected final ClusterClient clusterConnector;
    protected TransactionID tid;
    protected TransactionProcessor responseObserver;
    protected String transactionID;
    protected Bzs.Transaction remoteTransaction;
    //    private Bzs.Transaction transaction;
    Map<Integer, Bzs.TransactionResponse> remoteResponses = new ConcurrentHashMap<>();

    public RemoteOpProcessor(Integer cid, Integer rid, TransactionID tid, Bzs.Transaction transaction,
                             ClusterConnector clusterConnector) {
        this.cid = cid;
        this.rid = rid;
        this.clusterConnector = clusterConnector.getClusterClient();
        this.tid = tid;
        transactionID = String.format("%d:%d:%d:%d", cid, rid, tid.getEpochNumber(), tid.getSequenceNumber());
        remoteTransaction = Bzs.Transaction.newBuilder(transaction).setTransactionID(transactionID).build();
    }

    public void setResponseObserver(TransactionProcessor processor) {
        this.responseObserver = processor;
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
                if (messageType == MessageType.Prepare) {
                    Bzs.TransactionResponse response = clusterConnector.prepare(remoteTransaction, cid);
                    remoteResponses.put(cid, response);
                } else if (messageType == MessageType.Abort) {
                    Bzs.TransactionResponse response = clusterConnector.abort(remoteTransaction, cid);
                    remoteResponses.put(cid, response);
                }
            });
            t.start();
            remoteThreads.add(t);
        }
        return remoteThreads;
    }

    protected Set<Integer> getListOfClusterIDs() {

        Set<Integer> cidSet = new HashSet<>();

        for (int i = 0; i < remoteTransaction.getReadHistoryCount(); i++) {
            cidSet.add(remoteTransaction.getReadHistory(i).getClusterID());
        }
        for (int i = 0; i < remoteTransaction.getWriteOperationsCount(); i++) {
            cidSet.add(remoteTransaction.getWriteOperations(i).getClusterID());
        }
        return cidSet;
    }
}
