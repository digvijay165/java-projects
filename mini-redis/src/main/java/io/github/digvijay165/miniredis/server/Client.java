package io.github.digvijay165.miniredis.server;

/** Per-connection state. Only the event loop thread touches it. */
final class Client {
    private static final int BUFFER_SIZE = 16 * 1024;

    final ByteQueue in = new ByteQueue(BUFFER_SIZE);
    final ByteQueue out = new ByteQueue(BUFFER_SIZE);
    /** Set by QUIT or a protocol error: send what is queued, then disconnect. */
    boolean closeAfterWrite;
}
