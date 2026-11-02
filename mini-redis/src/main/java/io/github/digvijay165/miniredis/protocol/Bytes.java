package io.github.digvijay165.miniredis.protocol;

import java.nio.charset.StandardCharsets;

/**
 * Converts between bytes and strings with ISO-8859-1, which maps every byte to exactly one char
 * and back. Keys and values are binary in Redis, so this keeps them byte-for-byte intact.
 */
public final class Bytes {
    private Bytes() {
    }

    public static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.ISO_8859_1);
    }

    public static String str(byte[] data) {
        return new String(data, StandardCharsets.ISO_8859_1);
    }
}
