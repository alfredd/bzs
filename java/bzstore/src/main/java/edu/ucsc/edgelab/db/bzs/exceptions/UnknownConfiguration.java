package edu.ucsc.edgelab.db.bzs.exceptions;

public class UnknownConfiguration extends RuntimeException {
    public UnknownConfiguration(String format) {
        super(format);
    }

    public UnknownConfiguration(String message, Throwable cause) {
        super(message, cause);
    }
}
