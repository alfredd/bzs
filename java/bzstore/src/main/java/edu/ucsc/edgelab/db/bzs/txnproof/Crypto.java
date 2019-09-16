package edu.ucsc.edgelab.db.bzs.txnproof;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.replica.RSAKeyUtil;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class Crypto {

    public byte[] generateDigitalSignature(int replicaID, Bzs.TransactionResponse response) {
        byte[] message = response.toByteArray();
        return signMessage(replicaID, message);
    }

    private byte[] signMessage(int replicaID, byte[] message) {
        String privateKey = KeysCache.getPrivateKey(replicaID);
        if (privateKey!=null) {
            return RSAKeyUtil.generateSignature(message, privateKey);
        }
        return null;
    }

    private byte[] decryptMessage(int replicaID, byte[] message) {
        String publicKey = KeysCache.getPublicKey(replicaID);
        if (publicKey!=null) {
            return RSAKeyUtil.decryptSignature(message, publicKey);
        }
        return null;
    }



    public byte[] signLCE(int replicaID, int lce) {
        return signMessage(replicaID, ByteBuffer.allocate(4).putInt(lce).array());
    }

    /**
     *
     * @param replicaID
     * @param encryptedLCE
     * @return returns Integer.MIN_VALUE if data cannot be verified else returns last committed epoch.
     */
    public Integer verifyLCE(int replicaID, byte[] encryptedLCE) {
        byte[] lceBytes = decryptMessage(replicaID, encryptedLCE);
        if (lceBytes==null || lceBytes.length!=4) {
            return Integer.MIN_VALUE;
        }
        try {
            return ByteBuffer.wrap(lceBytes).getInt();
        } catch (Exception e) {
            return Integer.MIN_VALUE;
        }
    }



    public byte[] signDV(int replicaID, Map<Integer, Integer> dv) {
        Bzs.DVec depVec = Bzs.DVec.newBuilder().putAllDepVector(dv).build();
        return signMessage(replicaID, depVec.toByteArray());
    }

    public static void main(String[] args) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(15);
//        BigInteger b = BigInteger.valueOf(16);

        byte[] byteArray = byteBuffer.array(); // b.toByteArray();//
        System.out.println(Arrays.toString(byteArray));

        ByteBuffer reverseBuffer = ByteBuffer.wrap(byteArray);
//        ByteBuffer put = reverseBuffer.put(byteArray);
        System.out.println(reverseBuffer.getInt());
    }
}