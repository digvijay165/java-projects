package io.github.digvijay165.miniredis.protocol;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/** Reads RESP replies from a blocking stream. The benchmark client and the tests use it. */
public final class RespReader {
    /** An error reply, such as "ERR syntax error". */
    public record ErrorReply(String message) {
    }

    private final InputStream in;

    public RespReader(InputStream in) {
        this.in = in instanceof BufferedInputStream ? in : new BufferedInputStream(in, 64 * 1024);
    }

    /**
     * Reads one reply: a String (simple string), ErrorReply, Long (integer), byte[] (bulk string,
     * null for the null bulk string) or List&lt;Object&gt; (array, null for the null array).
     */
    public Object read() throws IOException {
        int type = in.read();
        if (type == -1) {
            throw new EOFException("connection closed");
        }
        String line = readLine();
        return switch (type) {
            case '+' -> line;
            case '-' -> new ErrorReply(line);
            case ':' -> Long.parseLong(line);
            case '$' -> readBulk(Integer.parseInt(line));
            case '*' -> readArray(Integer.parseInt(line));
            default -> throw new IOException("unexpected reply type '" + (char) type + "'");
        };
    }

    private byte[] readBulk(int length) throws IOException {
        if (length < 0) {
            return null;
        }
        byte[] data = in.readNBytes(length);
        if (data.length != length || in.read() != '\r' || in.read() != '\n') {
            throw new EOFException("truncated bulk string");
        }
        return data;
    }

    private List<Object> readArray(int count) throws IOException {
        if (count < 0) {
            return null;
        }
        List<Object> items = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            items.add(read());
        }
        return items;
    }

    private String readLine() throws IOException {
        StringBuilder line = new StringBuilder();
        while (true) {
            int b = in.read();
            if (b == -1) {
                throw new EOFException("connection closed mid-reply");
            }
            if (b == '\r') {
                if (in.read() != '\n') {
                    throw new IOException("expected LF after CR");
                }
                return line.toString();
            }
            line.append((char) b);
        }
    }
}
