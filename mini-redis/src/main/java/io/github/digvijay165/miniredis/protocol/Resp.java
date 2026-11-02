package io.github.digvijay165.miniredis.protocol;

import java.util.List;

import static io.github.digvijay165.miniredis.protocol.Bytes.bytes;

/** Encodes replies in RESP2, the protocol that redis-cli and every Redis client library speak. */
public final class Resp {
    public static final byte[] OK = simple("OK");
    public static final byte[] PONG = simple("PONG");
    public static final byte[] NULL_BULK = bytes("$-1\r\n");
    public static final byte[] NULL_ARRAY = bytes("*-1\r\n");
    public static final byte[] EMPTY_ARRAY = bytes("*0\r\n");

    private Resp() {
    }

    public static byte[] simple(String text) {
        return bytes("+" + text + "\r\n");
    }

    /** An error reply. The message starts with a code such as ERR or WRONGTYPE. */
    public static byte[] error(String message) {
        return bytes("-" + message.replace('\r', ' ').replace('\n', ' ') + "\r\n");
    }

    public static byte[] integer(long value) {
        return bytes(":" + value + "\r\n");
    }

    /** A binary-safe bulk string; null becomes the null bulk string. */
    public static byte[] bulk(byte[] data) {
        if (data == null) {
            return NULL_BULK;
        }
        byte[] header = bytes("$" + data.length + "\r\n");
        byte[] out = new byte[header.length + data.length + 2];
        System.arraycopy(header, 0, out, 0, header.length);
        System.arraycopy(data, 0, out, header.length, data.length);
        out[out.length - 2] = '\r';
        out[out.length - 1] = '\n';
        return out;
    }

    /** An array of bulk strings, which is also how a command is sent; null elements become null bulk strings. */
    public static byte[] array(List<byte[]> items) {
        byte[] header = bytes("*" + items.size() + "\r\n");
        byte[][] parts = new byte[items.size()][];
        int total = header.length;
        for (int i = 0; i < parts.length; i++) {
            parts[i] = bulk(items.get(i));
            total += parts[i].length;
        }
        byte[] out = new byte[total];
        System.arraycopy(header, 0, out, 0, header.length);
        int position = header.length;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, out, position, part.length);
            position += part.length;
        }
        return out;
    }
}
