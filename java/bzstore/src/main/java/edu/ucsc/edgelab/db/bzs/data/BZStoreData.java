package edu.ucsc.edgelab.db.bzs.data;

public class BZStoreData {

    public String value;

    public long version;

    public String digest;

    public BZStoreData() {
        value="";
        version=0;
        digest="";
    }

    public BZStoreData(String value, String digest) {
        this.value = value;
        this.digest = digest;
        version=0;
    }


    public BZStoreData(String value, long version, String digest) {
        this.value = value;
        this.version = version;
        this.digest = digest;
    }

    @Override
    public String toString() {
        return String.format("value: %s, version: %d, digest: %s",value,version,digest);
    }
}
