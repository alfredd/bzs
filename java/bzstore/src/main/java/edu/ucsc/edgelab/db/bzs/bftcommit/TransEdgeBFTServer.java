package edu.ucsc.edgelab.db.bzs.bftcommit;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.exceptions.InvalidCommitException;
import edu.ucsc.edgelab.db.bzs.replica.ID;
import edu.ucsc.edgelab.db.bzs.replica.Serializer;
import edu.ucsc.edgelab.db.bzs.replica.SmrLog;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TransEdgeBFTServer extends DefaultSingleRecoverable {

    private final Integer replicaID;
    private final Integer clusterID;
    private Logger logger = Logger.getLogger(TransEdgeBFTServer.class.getName());

    private Map<String, Bzs.TransactionBatchResponse> tbrCache = new LinkedHashMap<>();
    //    private Integer count;
    private Map<Integer, Bzs.SmrLogEntry> smrLogCache = new LinkedHashMap<>();
    private Map<Integer, Map<String, Bzs.DBData>> dbCache = new LinkedHashMap<>();

    public TransEdgeBFTServer(boolean isLeader) {
        logger.info("Starting BFT-Smart Server.");
//        count = 0;
        this.replicaID = ID.getReplicaID();
        this.clusterID = ID.getClusterID();
        new ServiceReplica(replicaID, this, this);
        logger.info("Started: BFT-Smart Server.");

    }

    @Override
    public byte[] appExecuteOrdered(byte[] request, MessageContext messageContext) {
        byte[] reply = ByteBuffer.allocate(4).putInt(-10).array();
        Bzs.TransactionBatch transactionBatch = null;
        try {
            transactionBatch = Bzs.TransactionBatch.newBuilder().mergeFrom(request).build();
        } catch (InvalidProtocolBufferException e) {
            logger.log(Level.SEVERE, "Exception occurred while parsing request: " + e.getLocalizedMessage(), e);
        }
        if (transactionBatch == null) {
            logger.log(Level.WARNING, "Received BFT request is null");
            return getRandomBytes();
        }
//        logger.info("Received BFT request: "+ transactionBatch.toString());

        Bzs.Operation operation = transactionBatch.getOperation();
        switch (operation) {
            case BFT_SMR_PREPARE:
                reply = prepareSMRBlock(transactionBatch);
                break;
            case BFT_SMR_COMMIT:
                reply = commitSMRBlock(transactionBatch);
                break;
            default:
                logger.log(Level.WARNING, "Cannot parse operation: " + operation);
        }
        return reply;
    }

    private byte[] commitSMRBlock(Bzs.TransactionBatch transactionBatch) {
        int epochNumber = transactionBatch.getEpochNumber();
        Bzs.SmrLogEntry logEntry = SmrLog.generateLogEntry(epochNumber);
        try {
            BZDatabaseController.commitSmrBlock(epochNumber, logEntry);
            commitDBCache(epochNumber);
        } catch (InvalidCommitException e) {
            logger.log(Level.SEVERE, "Could not commit to SMR log: " + logEntry, e);
        }
        return ByteBuffer.allocate(4).putInt(epochNumber).array();
    }

    private void commitDBCache(int epochNumber) throws InvalidCommitException {
        Map<String, Bzs.DBData> cache = dbCache.get(epochNumber);
        for (Map.Entry<String, Bzs.DBData> entry : cache.entrySet()) {
            BZDatabaseController.commitDBData(entry.getKey(), entry.getValue());
        }
    }

    private byte[] prepareSMRBlock(Bzs.TransactionBatch transactionBatch) {
        String batchID = transactionBatch.getID();
        int epochNumber = transactionBatch.getEpochNumber();
        SmrLog.createLogEntry(epochNumber);
        SmrLog.dependencyVector(epochNumber, transactionBatch.getDepVectorMap());
        SmrLog.updateEpochLCE(epochNumber, transactionBatch.getLce());
        List<Bzs.Transaction> abortList = new LinkedList<>();
        if (transactionBatch.getTxnBatchCount() > 0) {
            for (Bzs.ClusterPC txns : transactionBatch.getTxnBatchList()) {
                List<Bzs.Transaction> transactionsList = txns.getTransactionsList();
                switch (txns.getOperation()) {
                    case LOCAL_RWT:
                        SmrLog.localPrepared(epochNumber, transactionsList);
                        break;
                    case DRWT_PREPARE:
                        SmrLog.distributedPrepared(epochNumber, transactionsList);
                        break;
                    case DRWT_COMMIT:
                        SmrLog.committedDRWT(epochNumber, transactionsList);
                        break;
                    case TWO_PC_COMMIT:
                        SmrLog.twoPCCommitted(epochNumber, transactionsList, txns.getID());
                        break;
                    case TWO_PC_PREPARE:
                        SmrLog.twoPCPrepared(epochNumber, transactionsList, txns.getID());
                        break;
                }
                updateDBCacheForTransactions(epochNumber, transactionsList);
            }
        }
        Bzs.SmrLogEntry logEntry = SmrLog.generateLogEntry(epochNumber);
        return logEntry.toByteArray();
    }

    private void updateDBCacheForTransactions(int epochNumber, List<Bzs.Transaction> transactionsList) {
        for (Bzs.Transaction t : transactionsList) {
            for (Bzs.Write w : t.getWriteOperationsList()) {
                if (w.getClusterID() == clusterID)
                    updateDBCache(epochNumber, w.getKey(), w.getValue(), epochNumber);
            }
        }
    }


    private List<Bzs.Transaction> prepareLRWTxns(List<Bzs.Transaction> transactionsList, List<Bzs.Transaction> abortList) {
        List<Bzs.Transaction> prepared = new LinkedList<>();
        Serializer serializer = new Serializer();
        for (Bzs.Transaction txn : transactionsList) {
            if (serializer.serialize(txn))
                prepared.add(txn);
            else
                abortList.add(txn);
        }
        return prepared;
    }


    private void updateDBCache(Integer epochNumber, String key, String value, Integer version) {
        if (!dbCache.containsKey(epochNumber)) {
            dbCache.put(epochNumber, new LinkedHashMap<>());
        }
        dbCache.get(epochNumber).put(key, Bzs.DBData.newBuilder().setVersion(version).setValue(value).build());
    }


    private byte[] getRandomBytes() {
        return String.valueOf(new Random().nextInt()).getBytes();
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] appExecuteUnordered(byte[] transactions, MessageContext msgCtx) {
        return appExecuteOrdered(transactions, msgCtx);
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] getSnapshot() {
        return BZDatabaseController.getDBSnapshot();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void installSnapshot(byte[] state) {
        ByteArrayInputStream byteIn = new ByteArrayInputStream(state);
        BftUtil.installSH(byteIn, logger);
    }

    public static void main(String[] args) {
        Bzs.Transaction t1 = Bzs.Transaction.newBuilder()
                .setTransactionID("124")
                .setEpochNumber(45)
                .setStatus(Bzs.TransactionStatus.COMMITTED)
                .addWriteOperations(Bzs.Write.newBuilder().setKey("123").setValue("90").setClusterID(0).setReplicaID(3).build()).build();
        Bzs.Transaction t2 = Bzs.Transaction.newBuilder()
                .setTransactionID("124")
                .setEpochNumber(45)
                .setStatus(Bzs.TransactionStatus.COMMITTED)
                .addWriteOperations(Bzs.Write.newBuilder().setKey("123").setValue("90").setClusterID(0).setReplicaID(3).build()).build();
        System.out.println(Arrays.toString(t1.toByteArray()));
        System.out.println(Arrays.toString(t2.toByteArray()));
    }
}
