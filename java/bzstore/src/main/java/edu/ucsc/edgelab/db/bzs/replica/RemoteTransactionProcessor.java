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
        epochTimer.scheduleAtFixedRate(clusterConnector, 0, epochTimeInMS*1000*10);
    }

    public void processAsync(final TransactionID tid, final Bzs.Transaction request) {
        // Create a new thread for each tid
        // Get remote cluster ID
        // create a map of transaction=>clusterid=>status of prepare message
        // once responses of prepared messages start coming back, update the transaction status.
        // Once all responses are received call processor.prepared(tid)
        new Thread(() -> {

        }).start();
    }

    public void setObserver(TransactionProcessor transactionProcessor) {
        this.processor = transactionProcessor;
    }
}


class RemoteProcessor implements Runnable {

    private final Integer cid;
    private final Integer rid;
    private final ClusterClient clusterConnector;
    private Integer tid;
    private Bzs.Transaction transaction;
    Map<Integer, Bzs.TransactionResponse> remoteResponses = new ConcurrentHashMap<>();


    public RemoteProcessor(Integer tid, Bzs.Transaction transaction, Integer cid, Integer rid, ClusterConnector c) {
        this.tid = tid;
        this.transaction = transaction;
        this.cid = cid;
        this.rid = rid;
        this.clusterConnector = c.getClusterClient();
    }

    @Override
    public void run() {
        List<Integer> remoteCIDs = getListOfClusterIDs();
        List<Thread> remoteThreads = new LinkedList<>();

        for (int cid: remoteCIDs) {
            Thread t = new Thread(() -> {
                Bzs.TransactionResponse response = clusterConnector.commitPrepare(transaction, cid);
                remoteResponses.put(cid,response);
            });
            t.start();
            remoteThreads.add(t);
        }

        for (Thread t: remoteThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private List<Integer> getListOfClusterIDs() {

        LinkedList<Integer> cidList = new LinkedList<>();


        return cidList;
    }
}