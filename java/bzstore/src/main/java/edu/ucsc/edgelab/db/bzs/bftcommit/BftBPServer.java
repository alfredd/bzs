package edu.ucsc.edgelab.db.bzs.bftcommit;

import bftsmart.tom.MessageContext;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

public class BftBPServer extends BFTServer {

    private Logger logger = Logger.getLogger(BftBPServer.class.getName());

    private Map<Integer, Bzs.TransactionBatchResponse> tbrCache = new LinkedHashMap<>();
    private Integer count;

    public BftBPServer(int id) {
        super(id);
    }

    @Override
    public void installSnapshot(byte[] state) {
        ByteArrayInputStream byteIn = new ByteArrayInputStream(state);
        BftUtil.installSH(byteIn, logger);
    }

    @Override
    public byte[] getSnapshot() {
        return BZDatabaseController.getDBSnapshot();
    }

    @Override
    public byte[] appExecuteOrdered(byte[] bytes, MessageContext messageContext) {
        logger.info("Length of byte array received: "+ bytes.length);
        return ByteBuffer.allocate(4).putInt(1).array();
    }

    @Override
    public byte[] appExecuteUnordered(byte[] bytes, MessageContext messageContext) {
        return new byte[0];
    }
}
