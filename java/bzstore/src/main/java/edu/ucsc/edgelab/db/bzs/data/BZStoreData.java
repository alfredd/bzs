package edu.ucsc.edgelab.db.bzs.data;

public class BZStoreData {

    public String value;

    public int version;

//    public String digest;

    public BZStoreData(String value) {
        this.value = value;
    }

    public BZStoreData() {
        value="";
        version=0;
//        digest="";
    }

//    @Deprecated
//    public BZStoreData(String value, String digest) {
//        this.value = value;
//        this.digest = digest;
//        version=0;
//    }
//
//
//    public BZStoreData(String value, long version, String digest) {
//        this.value = value;
//        this.version = version;
//        this.digest = digest;
//    }

    public BZStoreData(String value, int version) {
        this.value = value;
        this.version = version;
    }



    @Override
    public String toString() {
        return String.format("value: %s, version: %d",value,version);
    }
}
