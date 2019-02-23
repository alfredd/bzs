package edu.ucsc.edgelab.grpcdemo;

import edu.ucsc.edgelab.db.bzs.EchoMessage;
import io.grpc.stub.StreamObserver;

import java.util.logging.Logger;

public class Worker {

    private static final Logger logger = Logger.getLogger(Worker.class.getName());
    void doThis(EchoMessage request, StreamObserver<EchoMessage> responseObserver, Observer echo) {
        logger.info("Entering doThis");
        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            EchoMessage message = EchoMessage.newBuilder().setMessage("EchoServer: "
                    + request.getMessage()).build();
            responseObserver.onNext(message);
            echo.done(request,responseObserver);
        }).start();
        logger.info("Exiting doThis");
    }
}
