package edu.ucsc.edgelab.db.bzs.bftcommit;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.exceptions.InvalidDataAccessException;
import edu.ucsc.edgelab.db.bzs.replica.Serializer;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.*;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BFTServer extends DefaultSingleRecoverable {

    private Logger logger = Logger.getLogger(BFTServer.class.getName());


    public BFTServer(int id) {
        logger.info("Starting BFT-Smart Server.");
        new ServiceReplica(id, this, this);
        logger.info("Started: BFT-Smart Server.");

    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] appExecuteOrdered(byte[] transactions, MessageContext msgCtx) {
        // TODO: Need to re-factor.
        byte[] reply;
        Serializer serializer = new Serializer();
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {

            Bzs.TransactionBatch transactionBatch =
                    Bzs.TransactionBatch.newBuilder().mergeFrom(byteOut.toByteArray()).build();

            Bzs.TransactionBatchResponse batchResponse;
            Bzs.TransactionBatchResponse.Builder batchResponseBuilder = Bzs.TransactionBatchResponse.newBuilder();
            if (transactionBatch.getTransactionsCount() > 0) {

                for (int transactionIndex = 0; transactionIndex < transactionBatch.getTransactionsCount(); transactionIndex++) {
                    Bzs.Transaction transaction = transactionBatch.getTransactions(transactionIndex);

                    if (!serializer.serialize(transaction)) {
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
                                .setVersion(bzStoreData.version + 1)
                                .setResponseDigest(generateHash(writeOp.getValue() + bzStoreData.digest)).build();
                        responseBuilder.setWriteResponses(i, writeResponse);

                    }
                    responseBuilder.setStatus(Bzs.TransactionStatus.COMMITTED);
                    response = responseBuilder.build();
                    batchResponseBuilder.addResponses(response);
                }

                batchResponse = batchResponseBuilder.build();

                logger.info("Completed generating response for transaction batch.");
                objOut.writeObject(batchResponse.toByteArray());
                objOut.flush();
                byteOut.flush();
                reply = byteOut.toByteArray();
            } else {
                if (transactionBatch.getRotransactionCount() > 0) {

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
                    objOut.writeObject(batchResponse.toByteArray());
                    objOut.flush();
                    byteOut.flush();
                    reply = byteOut.toByteArray();

                } else {
                    reply = getRandomBytes();
                }
            }

        } catch (IOException e) {
            logger.log(Level.SEVERE, "Exception occured when reading inputs. "+e.getLocalizedMessage(), e);
            reply = getRandomBytes();
        }
        return reply;
    }

    private BZStoreData getBzStoreData(String key) {
        BZStoreData bzStoreData;
        try {
            bzStoreData = BZDatabaseController.getlatest(key);

        } catch (InvalidDataAccessException e) {
            logger.log(Level.SEVERE, "Database access failed. " + e.getLocalizedMessage(), e);
            bzStoreData = new BZStoreData();
        }
        return bzStoreData;
    }

    private byte[] getRandomBytes() {
        return String.valueOf(new Random().nextInt()).getBytes();
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] appExecuteUnordered(byte[] transactions, MessageContext msgCtx) {
        return null;
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
        try {
            BZDatabaseController.initializeDb(byteIn);
        } catch (IOException | ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Error while installing snapshot", e);
        } finally {
            try {
                byteIn.close();
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error while closing byte stream during  installSnapshot", e);
            }
        }
    }

    private static String generateHash(String input) {
        return DigestUtils.md5Hex(input).toUpperCase();
    }

}
