package io.github.digvijay165.miniredis;

import io.github.digvijay165.miniredis.protocol.Bytes;
import io.github.digvijay165.miniredis.protocol.RespReader;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test helpers for decoded replies. */
public final class Replies {
    private Replies() {
    }

    /** Turns bulk strings into Strings (recursively inside arrays) so assertions read naturally. */
    public static Object decode(Object reply) {
        if (reply instanceof byte[] bytes) {
            return Bytes.str(bytes);
        }
        if (reply instanceof List<?> list) {
            return list.stream().map(Replies::decode).toList();
        }
        return reply;
    }

    public static void assertError(String prefix, Object reply) {
        RespReader.ErrorReply error = assertInstanceOf(RespReader.ErrorReply.class, reply);
        assertTrue(error.message().startsWith(prefix), () -> "expected an error starting with '" + prefix + "', got: " + error.message());
    }
}
