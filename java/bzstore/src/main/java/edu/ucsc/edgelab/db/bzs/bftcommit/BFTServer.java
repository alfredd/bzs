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
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BFTServer extends DefaultSingleRecoverable {

    private final Integer replicaID;
    private final boolean checkLocks;
    private final Integer clusterID;
    private Logger logger = Logger.getLogger(BFTServer.class.getName());

    private Map<String, Bzs.TransactionBatchResponse> tbrCache = new LinkedHashMap<>();
    //    private Integer count;
    private Map<Integer, Bzs.SmrLogEntry> smrLogCache = new LinkedHashMap<>();
    private Map<Integer, Map<String, Bzs.DBData>> dbCache = new LinkedHashMap<>();

    public BFTServer(boolean isLeader) {
        logger.info("Starting BFT-Smart Server.");
//        count = 0;
        this.replicaID = ID.getReplicaID();
        this.clusterID = ID.getClusterID();
        this.checkLocks = isLeader;
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
        logger.info("Received BFT request: "+ transactionBatch.toString());

        Bzs.Operation operation = transactionBatch.getOperation();
        switch (operation) {
            case BFT_PREPARE:
                reply = processBFTPrepare(transactionBatch).toByteArray();
                break;
            case BFT_SMR_PREPARE:
                reply = processSMRLogPrepare(transactionBatch);
                break;
            case BFT_SMR_COMMIT:
                reply = commitSMRLogEntry(transactionBatch);
                break;
            case BFT_ABORT:
        }
        return reply;
    }

    private byte[] commitSMRLogEntry(Bzs.TransactionBatch transactionBatch) {
        Integer epoch = Integer.decode(transactionBatch.getID());
        Bzs.SmrLogEntry smrLogEntry = smrLogCache.get(epoch);
        boolean committed = false;
        try {
            BZDatabaseController.commitSmrBlock(epoch, smrLogEntry);
            committed = true;
        } catch (InvalidCommitException e) {
            logger.log(Level.SEVERE,
                    String.format("Could not commit to SMR Log: Epoch(%d). Exception message: %s. Retrying...  ", epoch.intValue(),
                            e.getLocalizedMessage()), e);
            try {
                BZDatabaseController.commitSmrBlock(epoch, smrLogEntry);
                committed = true;
            } catch (InvalidCommitException e1) {
                logger.log(Level.SEVERE,
                        String.format("Could not commit to SMR Log: Epoch(%d):%s: ", epoch.intValue(), smrLogEntry.toString()) + e.getLocalizedMessage(), e);
            }
        }
        if (committed) {
            smrLogCache.remove(epoch);
            commitDBCache(epoch);
            BZDatabaseController.setEpochCount(epoch);
            BZDatabaseController.commitDepVector(smrLogEntry.getDepVectorMap());
            logger.info(String.format("Committed SMR log for epoch %d. Smr Log Entry: %s", epoch.intValue(), smrLogEntry.toString()));
        }

        return ByteBuffer.allocate(4).putInt(1).array();
    }

    private void commitDBCache(Integer epoch) {
        Map<String, Bzs.DBData> cache = dbCache.get(epoch);
        if (cache != null)
            for (Map.Entry<String, Bzs.DBData> entry : cache.entrySet()) {
                try {
                    BZDatabaseController.commitDBData(entry.getKey(), entry.getValue());
                } catch (InvalidCommitException e) {
                    logger.log(Level.SEVERE,
                            String.format("Could not commit data: %s:%s: ", entry.getKey(), entry.getValue().toString()) + e.getLocalizedMessage(),
                            e);
                }
            }
    }

    private byte[] processSMRLogPrepare(Bzs.TransactionBatch transactionBatch) {
        long startTime = System.currentTimeMillis();
        Bzs.SmrLogEntry smrLogEntry = transactionBatch.getSmrLogEntry();
        smrLogCache.put(smrLogEntry.getEpochNumber(), smrLogEntry);
        byte[] bytes = DigestUtils.md5(smrLogEntry.toByteArray());
        long duration = System.currentTimeMillis() - startTime;
        logger.info("Time to generate Hash: "+duration);
        return bytes;
    }

    private Bzs.TransactionBatchResponse processBFTPrepare(Bzs.TransactionBatch transactionBatch) {
        Integer epochNumber = Integer.decode(transactionBatch.getID());
        List<Bzs.TransactionResponse.Builder> responseList =
                getTransactionResponseBuilders(transactionBatch.getTransactionsList(), epochNumber);

        Bzs.TransactionBatchResponse.Builder tbr = Bzs.TransactionBatchResponse.newBuilder();

        for (Bzs.TransactionResponse.Builder builder : responseList) {
            tbr = tbr.addResponses(builder.build());
        }
        if (transactionBatch.getRemotePrepareTxnCount() > 0) {
            List<Bzs.ClusterPCResponse> cpcResponses = processClusterPrepareRequests(transactionBatch.getRemotePrepareTxnList(), epochNumber);
            for (Bzs.ClusterPCResponse cpcResponse : cpcResponses) {
                tbr = tbr.addRemotePrepareTxnResponse(cpcResponse);
            }
        }
        return tbr.build();
    }

    private List<Bzs.TransactionResponse.Builder> getTransactionResponseBuilders(List<Bzs.Transaction> transactionsList, int epochNumber) {
        List<Bzs.TransactionResponse.Builder> responseList = new LinkedList<>();
        Serializer serializer = new Serializer(clusterID, replicaID);
        for (Bzs.Transaction txn : transactionsList) {
            Bzs.TransactionStatus status = Bzs.TransactionStatus.ABORTED;
            if (serializer.serialize(txn)) {
                status = Bzs.TransactionStatus.PREPARED;
            }
            Bzs.TransactionResponse.Builder builder = Bzs.TransactionResponse.newBuilder()
                    .setTransactionID(txn.getTransactionID())
                    .setStatus(status);
            if (status.equals(Bzs.TransactionStatus.ABORTED)) {
                responseList.add(builder);
                continue;
            }
            for (Bzs.Write wOp : txn.getWriteOperationsList()) {
                if (wOp.getClusterID() == this.clusterID) {
                    Bzs.WriteResponse wresp = Bzs.WriteResponse.newBuilder()
                            .setWriteOperation(wOp)
                            .setVersion(txn.getEpochNumber())
                            .build();
                    builder = builder.addWriteResponses(wresp);

                    // Update DB data cache.
                    updateDBCache(epochNumber, wOp.getKey(), wOp.getValue(), txn.getEpochNumber());
                }
            }
            responseList.add(builder);
        }
        return responseList;
    }

    private List<Bzs.ClusterPCResponse> processClusterPrepareRequests(List<Bzs.ClusterPC> remotePrepareTxnList, Integer epochNumber) {
        List<Bzs.ClusterPCResponse> cpcResponses = new LinkedList<>();
        for (Bzs.ClusterPC cpc : remotePrepareTxnList) {
            Bzs.ClusterPCResponse.Builder cpcResponseBuilder = Bzs.ClusterPCResponse.newBuilder().setID(cpc.getID());
            if (cpc.getTransactionsCount() > 0) {
                List<Bzs.TransactionResponse.Builder> responseBuilders = getTransactionResponseBuilders(cpc.getTransactionsList(), epochNumber);
                for (Bzs.TransactionResponse.Builder builder : responseBuilders) {
                    cpcResponseBuilder = cpcResponseBuilder.addResponses(builder.build());
                }
                cpcResponses.add(cpcResponseBuilder.build());
            } else {
                cpcResponses.add(Bzs.ClusterPCResponse.newBuilder().setStatus(Bzs.OperationStatus.INVALID).build());
            }
        }
        return cpcResponses;
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

}
