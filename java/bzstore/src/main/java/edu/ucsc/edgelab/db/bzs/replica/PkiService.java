package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.PKIServiceGrpc;
import io.grpc.stub.StreamObserver;

public class PkiService extends PKIServiceGrpc.PKIServiceImplBase {

    private Integer replicaID;

    public PkiService(Integer replicaID) {
        this.replicaID = replicaID;
    }

    @Override
    public void getPrivateKey(Bzs.KeyType request, StreamObserver<Bzs.Key> responseObserver) {
        Bzs.Key resultKey = Bzs.Key.newBuilder().setKey(RSAKeyUtil.getPrivateKey(replicaID)).build();
        responseObserver.onNext(resultKey);
        responseObserver.onCompleted();
    }

    @Override
    public void getPublicKey(Bzs.KeyType request, StreamObserver<Bzs.Key> responseObserver) {
        Bzs.Key resultKey = Bzs.Key.newBuilder().setKey(RSAKeyUtil.getPublicKey(replicaID)).build();
        responseObserver.onNext(resultKey);
        responseObserver.onCompleted();
    }
}
