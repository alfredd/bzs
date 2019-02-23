package edu.ucsc.edgelab.grpcdemo;

import edu.ucsc.edgelab.db.bzs.EchoMessage;
import io.grpc.stub.StreamObserver;

public interface Observer {
    void done(EchoMessage request, StreamObserver<EchoMessage> responseObserver);
}
