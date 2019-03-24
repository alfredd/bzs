package edu.ucsc.edgelab.db.bzs.bftcommit;

import bftsmart.tom.MessageContext;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;

import java.io.ByteArrayInputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

public class BftBPServer extends DefaultRecoverable {

    private Logger logger = Logger.getLogger(BftBPServer.class.getName());

    private Map<Integer, Bzs.TransactionBatchResponse> tbrCache = new LinkedHashMap<>();
    private Integer count;

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
    public byte[][] appExecuteBatch(byte[][] bytes, MessageContext[] messageContexts, boolean b) {
        return new byte[0][];
    }

    @Override
    public byte[] appExecuteUnordered(byte[] bytes, MessageContext messageContext) {
        return new byte[0];
    }
}
