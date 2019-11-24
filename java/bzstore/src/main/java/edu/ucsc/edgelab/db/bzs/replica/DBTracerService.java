package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.DBTracerGrpc;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import io.grpc.stub.StreamObserver;

import java.util.logging.Logger;

public class DBTracerService extends DBTracerGrpc.DBTracerImplBase {

    private static final Logger log = Logger.getLogger(DBTracerService.class.getName());

    @Override
    public void getLatestLogEntry(Bzs.EpochNumber request, StreamObserver<Bzs.SmrLogEntry> responseObserver) {
//        int epochNumber = request.getNumber();
        int epochNumber = BZDatabaseController.getEpochCount();
        Bzs.SmrLogEntry smrEntry = BZDatabaseController.getSmrBlock(epochNumber);
        responseObserver.onNext(smrEntry);
        responseObserver.onCompleted();
    }

/*    @Override
    public void getNextLogEntry(Bzs.EpochNumber request, StreamObserver<Bzs.SmrLogEntry> responseObserver) {
        int epochNumber = request.getNumber();
        getLogEntryForEpochNumber(responseObserver, epochNumber + 1);
    }*/

    private void getLogEntryForEpochNumber(StreamObserver<Bzs.SmrLogEntry> responseObserver, int epochNumber) {
        int currentEpochNumber = BZDatabaseController.getEpochCount();
        if (epochNumber <= currentEpochNumber) {
            Bzs.SmrLogEntry smrEntry = BZDatabaseController.getSmrBlock(epochNumber);
            responseObserver.onNext(smrEntry);
        } else {
            responseObserver.onNext(Bzs.SmrLogEntry.newBuilder().setEpochNumber(-1).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void getLogEntry(Bzs.EpochNumber request, StreamObserver<Bzs.SmrLogEntry> responseObserver) {
        int epochNumber = request.getNumber();
        getLogEntryForEpochNumber(responseObserver, epochNumber);
    }
}
