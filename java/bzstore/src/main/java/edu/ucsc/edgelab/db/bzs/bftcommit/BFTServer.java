package edu.ucsc.edgelab.db.bzs.bftcommit;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.exceptions.InvalidCommitException;
import edu.ucsc.edgelab.db.bzs.replica.Serializer;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BFTServer extends DefaultSingleRecoverable {

    private Logger logger = Logger.getLogger(BFTServer.class.getName());

    private Map<Integer, Bzs.TransactionBatchResponse> tbrCache = new LinkedHashMap<>();
//    private Integer count;

    public BFTServer(int id) {
        logger.info("Starting BFT-Smart Server.");
//        count = 0;
        new ServiceReplica(id, this, this);
        logger.info("Started: BFT-Smart Server.");

    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] appExecuteOrdered(byte[] transactions, MessageContext msgCtx) {
        // TODO: Need to re-factor.
        byte[] reply;
        try {

            Bzs.TransactionBatch transactionBatch =
//                    Bzs.TransactionBatch.newBuilder().mergeFrom(byteOut.toByteArray()).build();
                    Bzs.TransactionBatch.newBuilder().mergeFrom(transactions).build();
//            logger.info("Transaction batch: " + transactionBatch.toString());
            Bzs.TransactionBatchResponse batchResponse;
            Bzs.TransactionBatchResponse.Builder batchResponseBuilder = Bzs.TransactionBatchResponse.newBuilder();
            if (transactionBatch.getOperation().equals(Bzs.Operation.BFT_PREPARE) &&
                    transactionBatch.getTransactionsCount() > 0) {
//                count += 1;
//                Integer counter = count;
                Serializer serializer = new Serializer();
                int epochId = transactionBatch.getID();
                logger.info("Processing transaction batch: "+epochId);

                for (int transactionIndex = 0; transactionIndex < transactionBatch.getTransactionsCount(); transactionIndex++) {
                    Bzs.Transaction transaction = transactionBatch.getTransactions(transactionIndex);

                    if (!serializer.serialize(transaction)) {
                        logger.log(Level.WARNING,
                                "Returning random bytes. Could not serialize the transaction: " + transaction.toString());
                        return getRandomBytes();
                    }
//                    logger.info("Generating transaction hash for consensus.");
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
                                .setVersion(bzStoreData.version + 1)
                                .setResponseDigest(generateHash(writeOp.getValue() + bzStoreData.digest)).build();
                        responseBuilder.addWriteResponses(writeResponse);

                    }
                    responseBuilder.setStatus(Bzs.TransactionStatus.COMMITTED);
                    response = responseBuilder.build();
                    batchResponseBuilder.addResponses(response);
                }
                serializer.resetEpoch();
                batchResponseBuilder.setID(epochId);
                batchResponse = batchResponseBuilder.build();
                tbrCache.put(epochId, batchResponse);
/*                logger.info("Completed generating response for transaction batch with batch id: " + epochId + ". " +
                        "Transaction batch data: " + batchResponse.toString()+
                        ". Transaction count= "+batchResponse.getResponsesCount());*/
                reply = batchResponse.toByteArray();
/*                List<Integer> inputsBytes = new LinkedList<>();
                for (byte b : reply) {
                    inputsBytes.add(b & 0xFF);
                }
                logger.info("Response Byte array: " + inputsBytes);*/
            } else if ( transactionBatch.getOperation().equals(Bzs.Operation.BFT_PREPARE) &&
                    transactionBatch.getRotransactionCount() > 0) {

                for (int i = 0; i < transactionBatch.getRotransactionCount(); i++) {
                    Bzs.ROTransaction roTransaction = transactionBatch.getRotransaction(i);

                    Bzs.ROTransactionResponse.Builder roTransactionResponseBuilder =
                            Bzs.ROTransactionResponse.newBuilder();

                    for (Bzs.Read readOp : roTransaction.getReadOperationsList()) {
                        BZStoreData storeData = getBzStoreData(readOp.getKey());
                        Bzs.ReadResponse response = Bzs.ReadResponse.newBuilder()
                                .setKey(readOp.getKey())
                                .setValue(storeData.value)
                                .setVersion(storeData.version)
                                .setStatus(
                                        readOp.getKey() == null ? Bzs.OperationStatus.INVALID :
                                                Bzs.OperationStatus.SUCCESS
                                )
                                .setResponseDigest(storeData.digest).buildPartial();
                        roTransactionResponseBuilder.addReadResponses(response);
                    }
                    batchResponseBuilder.addReadresponses(roTransactionResponseBuilder.build());
                }
                batchResponse = batchResponseBuilder.build();

                logger.info("Completed generating response for transaction batch.");
                reply = batchResponse.toByteArray();

            } else if (transactionBatch.getOperation().equals(Bzs.Operation.BFT_COMMIT)) {
                logger.info("====================================");
                logger.info("======Performing BFT DB Commit======");
                logger.info("====================================");
//                Bzs.BFTCommit commitTransactions = transactionBatch.getBftCommit();
                int id = transactionBatch.getID();
                logger.info("Processing db commit transactions with batch id: " + id);
//                logger.info("CommitTransactions: " + commitTransactions.toString());
                if (!tbrCache.containsKey(id)) {
                    logger.log(Level.WARNING, "Transactions are not present in cache for id: " + id + ". Transaction " +
                            "will abort.");
                    return getRandomBytes();
                }
                Bzs.TransactionBatchResponse cached = tbrCache.get(id);
//                logger.info("CachedResponse: "+cached.toString());
                // Commit to database.
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
//                            logger.info("Committing write : " + writeResponse);
                            BZDatabaseController.commit(writeResponse.getKey(), data);
                            committedKeys.add(writeResponse.getKey());
//                            logger.info("Committed write : " + writeResponse);

                        } catch (InvalidCommitException e) {
                            logger.log(Level.WARNING, "Commit failed for transaction: " + response.toString() + ". " +
                                    "All" +
                                    " committed keys will be rolled back and the transaction will be aborted.");
                            BZDatabaseController.rollbackForKeys(committedKeys);

                            committed = false;
                            break;
                        }
                    }
                    if (!committed) {
                        return getRandomBytes();
                    }
                }
                logger.info("Data committed. Returning response.");
//                Bzs.BFTCommitResponse bftCommitResponse1 = Bzs.BFTCommitResponse.newBuilder()
//                        .addAllResponses(commitTransactions.getTransactionsList()).build();
//
//                Bzs.TransactionBatchResponse commitResponse2 = Bzs.TransactionBatchResponse.newBuilder()
//                        .setBftCommitResponse(bftCommitResponse1)
//                        .setID(id)
//                        .build();
//                logger.info("Response object: "+commitResponse2.toString());
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
        String hash = input;
//        String hash = DigestUtils.md5Hex(input).toUpperCase();
//        long duration = System.nanoTime() - starttime;
//        logger.info("Hash generated in " + duration + "nanosecs");

        return hash;

    }

}
