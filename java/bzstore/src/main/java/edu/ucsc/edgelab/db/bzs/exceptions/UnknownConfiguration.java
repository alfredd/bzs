package edu.ucsc.edgelab.db.bzs.exceptions;

public class UnknownConfiguration extends Throwable {
    public UnknownConfiguration(String format) {
        super(format);
    }
}
