package edu.ucsc.edgelab.db.bzs.replica;

import javax.crypto.Cipher;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;

public class RSAKeyUtil {
    static String path = "config" + System.getProperty("file.separator") + "keys" + System.getProperty("file.separator");

    public static String getPublicKey(int replicaID) {
        String key = "";
        String filePath = path + "publickey" + replicaID;
        try {
            key = new String(Files.readAllBytes(Paths.get(filePath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return key;
    }

    public static String getPrivateKey(int replicaID) {
        String key = "";
        String filePath = path + "privatekey" + replicaID;
        try {
            key = new String(Files.readAllBytes(Paths.get(filePath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return key;
    }

    public static byte[] generateSignature(int replicaId, byte[] message) {

        byte[] result = null;
        byte[] private_key = Base64.getDecoder().decode(getPrivateKey(replicaId));
        try {
            PrivateKey puk =
                    KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(private_key));
            Cipher cipher = Cipher.getInstance("RSA");
            cipher.init(Cipher.ENCRYPT_MODE, puk);
            result = cipher.doFinal(message);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return result;
    }

    public static byte[] decryptSignature(int replicaId, byte[] message) {
        byte[] result = null;
        byte[] public_key = Base64.getDecoder().decode(getPublicKey(replicaId));

        try {
            PublicKey puk =
                    KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(public_key));
            Cipher cipher = Cipher.getInstance("RSA");
            cipher.init(Cipher.DECRYPT_MODE, puk);
            result = cipher.doFinal(message);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return result;

    }

    public static void main(String args[]) throws Exception {
        PrintStream out = System.out;
        out.println(System.getProperty("user.dir"));
        String plain_text = "Hello Worlds";
        out.println(plain_text);
        String pub = getPrivateKey(0);
        String puk = getPublicKey(0);
        byte[] plain_bytes = plain_text.getBytes("UTF-8");
        out.println("Plain text: "+Arrays.toString(plain_bytes));
        byte[] sig = generateSignature(0, plain_bytes);
        System.out.println("Generated signature: " + Arrays.toString(sig));
        byte[] decrypted = decryptSignature(0, sig);
        System.out.println("Decrypted data: "+Arrays.toString(decrypted));
        String decrypted_text = new String(decrypted, "UTF-8");
        System.out.println("Decrypted text: "+decrypted_text);
    }


}
