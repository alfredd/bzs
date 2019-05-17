package edu.ucsc.edgelab.db.bzs.bftcommit;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.data.MerkleBTreeManager;
import edu.ucsc.edgelab.db.bzs.exceptions.InvalidCommitException;
import edu.ucsc.edgelab.db.bzs.replica.Serializer;

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

    public BFTServer(Integer clusterID, int replicaID, boolean isLeader) {
        logger.info("Starting BFT-Smart Server.");
//        count = 0;
        this.replicaID = replicaID;
        this.clusterID = clusterID;
        this.checkLocks = isLeader;
        new ServiceReplica(replicaID, this, this);
        logger.info("Started: BFT-Smart Server.");

    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] appExecuteOrdered(byte[] transactions, MessageContext msgCtx) {
        // TODO: Need to re-factor.
        byte[] reply;
        try {

            Bzs.TransactionBatch transactionBatch =
                    Bzs.TransactionBatch.newBuilder().mergeFrom(transactions).build();
            Bzs.TransactionBatchResponse batchResponse;
            Bzs.TransactionBatchResponse.Builder batchResponseBuilder = Bzs.TransactionBatchResponse.newBuilder();
            if (transactionBatch.getOperation().equals(Bzs.Operation.BFT_PREPARE) &&
                    transactionBatch.getTransactionsCount() > 0) {
                Serializer serializer = new Serializer(!checkLocks);
                serializer.setClusterID(clusterID);
                String epochId = transactionBatch.getID();
                Integer versionNumber = Integer.decode(epochId.split(":")[0]);
                logger.info("Processing transaction batch: " + epochId);

                for (int transactionIndex = 0; transactionIndex < transactionBatch.getTransactionsCount(); transactionIndex++) {
                    Bzs.Transaction transaction = transactionBatch.getTransactions(transactionIndex);

                    if (!serializer.serialize(transaction)) {
                        logger.log(Level.WARNING,
                                "Returning random bytes. Could not serialize the transaction: " + transaction.toString());
                        return getRandomBytes();
                    }
                    Bzs.TransactionResponse response;
                    Bzs.TransactionResponse.Builder responseBuilder = Bzs.TransactionResponse.newBuilder();
                    for (int i = 0; i < transaction.getWriteOperationsCount(); i++) {
                        Bzs.Write writeOp = transaction.getWriteOperations(i);
                        BZStoreData bzStoreData;
                        String key = writeOp.getKey();
                        bzStoreData = getBzStoreData(key);
                        Bzs.WriteResponse writeResponse = Bzs.WriteResponse.newBuilder()
                                .setKey(key)
                                .setValue(writeOp.getValue())
                                .setVersion(versionNumber)
                                .setResponseDigest(generateHash(writeOp.getValue() + bzStoreData.digest)).build();
                        responseBuilder.addWriteResponses(writeResponse);

                    }
                    responseBuilder.setStatus(Bzs.TransactionStatus.COMMITTED).setTransactionID(transaction.getTransactionID());
                    response = responseBuilder.build();
                    batchResponseBuilder.addResponses(response);
                }
                serializer.resetEpoch();
                batchResponseBuilder.setID(epochId);
                batchResponse = batchResponseBuilder.build();
                tbrCache.put(epochId, batchResponse);
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
                                writeResponse.getValue(),
                                writeResponse.getVersion(),
                                writeResponse.getResponseDigest());
                        try {
                            String key = writeResponse.getKey();
                            BZDatabaseController.commit(key, data);
                            committedKeys.add(key);
                            MerkleBTreeManager.insert(key,
                                    Bzs.DBData.newBuilder()
                                            .setValue(data.value)
                                            .setVersion(data.version)
                                            .build(),
                                    true);

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
