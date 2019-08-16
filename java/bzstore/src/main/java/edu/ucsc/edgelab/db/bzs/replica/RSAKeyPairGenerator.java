package edu.ucsc.edgelab.db.bzs.replica;

import org.apache.commons.codec.binary.Base64;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.security.*;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;

import org.apache.commons.codec.binary.Base64;

public class RSAKeyPairGenerator {
    //
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//
    public RSAKeyPairGenerator() {
    }

    public void run(int id) throws Exception {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(512);
        KeyPair kp = keyGen.generateKeyPair();
        PublicKey puk = kp.getPublic();
        PrivateKey prk = kp.getPrivate();
        this.saveToFile(id, puk, prk);
    }

    private void saveToFile(int id, PublicKey puk, PrivateKey prk) throws Exception {
        String path = "config" + System.getProperty("file.separator") + "keys" + System.getProperty("file.separator");
        BufferedWriter w = new BufferedWriter(new FileWriter(path + "publickey" + id, false));
        w.write(this.getKeyAsString(puk));
        w.flush();
        w.close();
        w = new BufferedWriter(new FileWriter(path + "privatekey" + id, false));
        w.write(this.getKeyAsString(prk));
        w.flush();
        w.close();
    }

    private String getKeyAsString(Key key) {
        byte[] keyBytes = key.getEncoded();
        return Base64.encodeBase64String(keyBytes);
    }

    public static void main(String[] args) {
        try {
            (new RSAKeyPairGenerator()).run(Integer.parseInt(args[0]));
        } catch (Exception var2) {
            System.out.println(var2.getMessage());
            System.err.println("Use: RSAKeyPairGenerator <id> <key size>");
        }

    }
}

