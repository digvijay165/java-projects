package io.github.digvijay165.miniredis.protocol;

/** A client sent bytes that are not a valid request. */
public final class ProtocolException extends Exception {
    private static final long serialVersionUID = 1L;

    public ProtocolException(String message) {
        super(message);
    }
}
