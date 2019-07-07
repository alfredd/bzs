package edu.ucsc.edgelab.db.bzs.bftcommit;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.data.MerkleBTreeManager;
import edu.ucsc.edgelab.db.bzs.exceptions.InvalidCommitException;
import edu.ucsc.edgelab.db.bzs.replica.ID;
import edu.ucsc.edgelab.db.bzs.replica.Serializer;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
            return getRandomBytes();
        }


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
        Bzs.SmrLogEntry smrLogEntry = transactionBatch.getSmrLogEntry();
        smrLogCache.put(smrLogEntry.getEpochNumber(), smrLogEntry);
        return DigestUtils.md5(smrLogEntry.toByteArray());
    }

    private Bzs.TransactionBatchResponse processBFTPrepare(Bzs.TransactionBatch transactionBatch) {
        List<Bzs.TransactionResponse.Builder> responseList = getTransactionResponseBuilders(transactionBatch.getTransactionsList());

        Bzs.TransactionBatchResponse.Builder tbr = Bzs.TransactionBatchResponse.newBuilder();

        for (Bzs.TransactionResponse.Builder builder : responseList) {
            tbr = tbr.addResponses(builder.build());
        }
        if (transactionBatch.getRemotePrepareTxnCount() > 0) {
            List<Bzs.ClusterPCResponse> cpcResponses = processClusterPrepareRequests(transactionBatch.getRemotePrepareTxnList());
            for (Bzs.ClusterPCResponse cpcResponse : cpcResponses) {
                tbr = tbr.addRemotePrepareTxnResponse(cpcResponse);
            }
        }
        return tbr.build();
    }

    private List<Bzs.TransactionResponse.Builder> getTransactionResponseBuilders(List<Bzs.Transaction> transactionsList) {
        List<Bzs.TransactionResponse.Builder> responseList = new LinkedList<>();
        Serializer serializer = new Serializer(clusterID, replicaID);
        for (Bzs.Transaction txn : transactionsList) {
            TransactionID tid = TransactionID.getTransactionID(txn.getTransactionID());
            Bzs.TransactionStatus status = Bzs.TransactionStatus.ABORTED;
            if (serializer.serialize(txn)) {
                status = Bzs.TransactionStatus.PREPARED;
            }
            Bzs.TransactionResponse.Builder builder = Bzs.TransactionResponse.newBuilder()
                    .setTransactionID(tid.getTiD())
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
                    updateDBCache(wOp.getKey(), wOp.getValue(), txn.getEpochNumber());
                }
            }
            responseList.add(builder);
        }
        return responseList;
    }

    private List<Bzs.ClusterPCResponse> processClusterPrepareRequests(List<Bzs.ClusterPC> remotePrepareTxnList) {
        List<Bzs.ClusterPCResponse> cpcResponses = new LinkedList<>();
        for (Bzs.ClusterPC cpc : remotePrepareTxnList) {
            Bzs.ClusterPCResponse.Builder cpcResponseBuilder = Bzs.ClusterPCResponse.newBuilder();
            if (cpc.getTransactionsCount() > 0) {
                List<Bzs.TransactionResponse.Builder> responseBuilders = getTransactionResponseBuilders(cpc.getTransactionsList());
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

    private void updateDBCache(String key, String value, Integer epochNumber) {
        if (!dbCache.containsKey(epochNumber)) {
            dbCache.put(epochNumber, new LinkedHashMap<>());
        }
        dbCache.get(epochNumber).put(key, Bzs.DBData.newBuilder().setVersion(epochNumber).setValue(value).build());
    }


    public byte[] appExecuteOrdered2(byte[] transactions, MessageContext msgCtx) {
        // TODO: Need to re-factor.
        byte[] reply;
        try {

            Bzs.TransactionBatch transactionBatch =
                    Bzs.TransactionBatch.newBuilder().mergeFrom(transactions).build();
            Bzs.TransactionBatchResponse batchResponse;
            Bzs.TransactionBatchResponse.Builder batchResponseBuilder = Bzs.TransactionBatchResponse.newBuilder();
            if (transactionBatch.getOperation().equals(Bzs.Operation.BFT_PREPARE) &&
                    transactionBatch.getTransactionsCount() > 0) {
                Serializer serializer = new Serializer(clusterID, replicaID);
                String epochId = transactionBatch.getID();
                Integer versionNumber = Integer.decode(epochId.split(":")[0]);
                logger.info("Processing transaction batch from Epoch: " + epochId + ". Transaction batch: " + transactionBatch.toString());

                for (int transactionIndex = 0; transactionIndex < transactionBatch.getTransactionsCount(); transactionIndex++) {
                    Bzs.Transaction transaction = transactionBatch.getTransactions(transactionIndex);
                    logger.info("Verifying if transaction is serializable.");
                    if (!serializer.serialize(transaction)) {
                        logger.log(Level.WARNING,
                                "Returning random bytes. Could not serialize the transaction: " + transaction.toString());
                        return getRandomBytes();
                    }
                    logger.info("Transaction is serializable. Processing BFT prepare: " + transaction);
                    Bzs.TransactionResponse response;
                    Bzs.TransactionResponse.Builder responseBuilder = Bzs.TransactionResponse.newBuilder();
                    for (int i = 0; i < transaction.getWriteOperationsCount(); i++) {
                        Bzs.Write writeOp = transaction.getWriteOperations(i);
//                        if (writeOp.getClusterID()==clusterID) {
                        BZStoreData bzStoreData;
                        String key = writeOp.getKey();
                        bzStoreData = getBzStoreData(key);
                        Bzs.WriteResponse writeResponse = Bzs.WriteResponse.newBuilder()
                                .setWriteOperation(writeOp)
                                .setVersion(versionNumber)
                                .setResponseDigest(generateHash(writeOp.getValue())).build();

                        responseBuilder.addWriteResponses(writeResponse);
//                        }

                    }
                    responseBuilder.setStatus(Bzs.TransactionStatus.COMMITTED).setTransactionID(transaction.getTransactionID());
                    response = responseBuilder.build();
                    batchResponseBuilder.addResponses(response);
                }
                serializer.resetEpoch();
                batchResponseBuilder.setID(epochId);
                batchResponse = batchResponseBuilder.build();
                tbrCache.put(epochId, batchResponse);
                logger.info("Response prepared: " + batchResponse.toString());
                reply = batchResponse.toByteArray();
            } else if (transactionBatch.getOperation().equals(Bzs.Operation.BFT_COMMIT)) {
                String id = transactionBatch.getID();
                if (!tbrCache.containsKey(id)) {
                    logger.log(Level.WARNING, "Transactions are not present in cache for replicaID: " + id + ". " +
                            "Transaction " +
                            "will abort.");
                    return getRandomBytes();
                }
                Bzs.TransactionBatchResponse cached = tbrCache.get(id);
                logger.info("Beginning the process to write into the database.");
                List<String> committedKeys = new LinkedList<>();
                for (int i = 0; i < cached.getResponsesCount(); i++) {
                    Bzs.TransactionResponse response = cached.getResponses(i);
                    boolean committed = true;
                    for (Bzs.WriteResponse writeResponse : response.getWriteResponsesList()) {
                        BZStoreData data = new BZStoreData(
                                writeResponse.getWriteOperation().getValue(),
                                writeResponse.getVersion());
                        try {
                            if (writeResponse.getWriteOperation().getClusterID() == this.clusterID) {
                                logger.info(String.format("Writing  to DB {key, value, version} = {%s, %s, %d}",
                                        writeResponse.getWriteOperation().getKey(),
                                        writeResponse.getWriteOperation().getValue(), writeResponse.getVersion()));
                                String key = writeResponse.getWriteOperation().getKey();
                                BZDatabaseController.commit(key, data);
                                committedKeys.add(key);
                                MerkleBTreeManager.insert(key,
                                        Bzs.DBData.newBuilder()
                                                .setValue(data.value)
                                                .setVersion(data.version)
                                                .build(),
                                        true);
                            }

                        } catch (InvalidCommitException e) {
//                            logger.log(Level.WARNING, "Commit failed for transaction: " + response.toString() + ". " +
//                                    "All" +
//                                    " committed keys will be rolled back and the transaction will be aborted.");
//                            BZDatabaseController.rollbackForKeys(committedKeys);

                            committed = false;
                            break;
                        }
                    }
                    if (!committed) {
                        return getRandomBytes();
                    }
                }
                logger.info("Data committed. Returning response.");
                reply = ByteBuffer.allocate(4).putInt(1).array();
            } else if (transactionBatch.getOperation().equals(Bzs.Operation.BFT_ABORT)) {
                String id = transactionBatch.getID();
                if (tbrCache.containsKey(id)) {
                    Bzs.TransactionBatchResponse storedBatch = tbrCache.remove(id);
                    logger.log(Level.INFO, "Transaction abort received for: " + id + ". Transaction " +
                            "will be aborted. Transaction details: " + storedBatch.toString());
                }
                reply = ByteBuffer.allocate(4).putInt(1).array();
            } else {
                logger.log(Level.WARNING,
                        "No matches found. Aborting consensus. Input was: " + transactionBatch.toString());
                reply = getRandomBytes();
            }


        } catch (IOException e) {
            logger.log(Level.SEVERE, "Exception occured when reading inputs. " + e.getLocalizedMessage(), e);
            reply = getRandomBytes();
        }
        return reply;
    }

    private BZStoreData getBzStoreData(String key) {

        return BZDatabaseController.getlatest(key);
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

    private String generateHash(final String input) {
//        long starttime = System.nanoTime();
        String hash = "";
//        String hash = DigestUtils.md5Hex(input).toUpperCase();
//        long duration = System.nanoTime() - starttime;
//        logger.info("Hash generated in " + duration + "nanosecs");

        return hash;

    }

}
