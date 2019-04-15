package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterClient;
import edu.ucsc.edgelab.db.bzs.cluster.ClusterConnector;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static edu.ucsc.edgelab.db.bzs.replica.EpochManager.*;

public class RemoteTransactionProcessor {

    public Integer clusterID;
    public Integer replicaID;
    private TransactionProcessor processor;
    private ClusterConnector clusterConnector;

    public RemoteTransactionProcessor(Integer clusterID, Integer replicaID) {
        this.clusterID = clusterID;
        this.replicaID = replicaID;

        Timer epochTimer = new Timer("TimedEpochMaintainer", true);
        Integer epochTimeInMS = getEpochTimeInMS();
        clusterConnector = new ClusterConnector(this.clusterID);
        epochTimer.scheduleAtFixedRate(clusterConnector, 0, epochTimeInMS * 1000 * 10);
    }

    public void processAsync(final TransactionID tid, final Bzs.Transaction request) {
        // Create a new thread for each tid
        // Get remote cluster ID
        // create a map of transaction=>clusterid=>status of prepare message
        // once responses of prepared messages start coming back, update the transaction status.
        // Once all responses are received call processor.prepared(tid)
        RemoteProcessor remoteProcessor = new RemoteProcessor(tid,request,clusterID,replicaID, clusterConnector);
        remoteProcessor.setResponseObserver(processor);
        new Thread(remoteProcessor).start();
    }

    public void setObserver(TransactionProcessor transactionProcessor) {
        this.processor = transactionProcessor;
    }
}


class RemoteProcessor implements Runnable {

    private final Integer cid;
    private final Integer rid;
    private final ClusterClient clusterConnector;
    private TransactionID tid;
    private Bzs.Transaction transaction;
    Map<Integer, Bzs.TransactionResponse> remoteResponses = new ConcurrentHashMap<>();
    private TransactionProcessor responseObserver;


    public RemoteProcessor(TransactionID tid, Bzs.Transaction transaction, Integer cid, Integer rid, ClusterConnector c) {
        this.tid = tid;
        this.transaction = transaction;
        this.cid = cid;
        this.rid = rid;
        this.clusterConnector = c.getClusterClient();
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
                    Bzs.TransactionResponse response = clusterConnector.commitPrepare(transaction, cid);
                    remoteResponses.put(cid, response);
                } else if(messageType==MessageType.Abort) {
                    Bzs.TransactionResponse response = clusterConnector.abort(transaction, cid);
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

        for (int i =0;i<transaction.getReadHistoryCount();i++) {
            cidSet.add(transaction.getReadHistory(i).getClusterID());
        }
        for (int i =0;i<transaction.getWriteOperationsCount();i++) {
            cidSet.add(transaction.getWriteOperations(i).getClusterID());
        }
        return cidSet;
    }
}