package edu.ucsc.edgelab.grpcdemo;

import edu.ucsc.edgelab.db.bzs.EchoGrpc;
import edu.ucsc.edgelab.db.bzs.EchoMessage;
import io.grpc.stub.StreamObserver;

import java.util.logging.Logger;

class EchoImpl extends EchoGrpc.EchoImplBase implements  Observer{

    private static final Logger logger = Logger.getLogger(EchoImpl.class.getName());
    @Override
    public void echoThis(EchoMessage request, StreamObserver<EchoMessage> responseObserver) {
        logger.info("Entering echoThis");
        new Worker().doThis(request,responseObserver, this);
        logger.info("Exiting echoThis");
    }

    @Override
    public void done(EchoMessage request, StreamObserver<EchoMessage> responseObserver) {
        logger.info("Entering done");
        responseObserver.onCompleted();
        logger.info("Exiting done");
    }
}
