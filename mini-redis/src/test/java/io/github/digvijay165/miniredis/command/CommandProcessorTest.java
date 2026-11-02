package io.github.digvijay165.miniredis.command;

import io.github.digvijay165.miniredis.Replies;
import io.github.digvijay165.miniredis.protocol.Bytes;
import io.github.digvijay165.miniredis.protocol.RespReader;
import io.github.digvijay165.miniredis.store.DataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.github.digvijay165.miniredis.Replies.assertError;
import static io.github.digvijay165.miniredis.protocol.Bytes.bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommandProcessorTest {
    private final AtomicLong clock = new AtomicLong(1_000_000);
    private final DataStore store = new DataStore(clock::get);
    private final CommandProcessor redis = new CommandProcessor(store);
    private final List<String> propagated = new ArrayList<>();

    @BeforeEach
    void captureWrites() {
        redis.setPropagation(command -> propagated.add(command.stream().map(Bytes::str).collect(Collectors.joining(" "))));
    }

    private Object call(String... args) {
        byte[] reply = redis.execute(Arrays.stream(args).map(Bytes::bytes).toList());
        try {
            return Replies.decode(new RespReader(new ByteArrayInputStream(reply)).read());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void pingAndEcho() {
        assertEquals("PONG", call("PING"));
        assertEquals("hello", call("PING", "hello"));
        assertEquals("hi there", call("ECHO", "hi there"));
    }

    @Test
    void setAndGet() {
        assertEquals("OK", call("SET", "name", "digvijay"));
        assertEquals("digvijay", call("GET", "name"));
        assertNull(call("GET", "missing"));
    }

    @Test
    void setNxOnlySetsNewKeysAndXxOnlyExistingOnes() {
        assertEquals("OK", call("SET", "k", "1", "NX"));
        assertNull(call("SET", "k", "2", "NX"));
        assertEquals("1", call("GET", "k"));
        assertNull(call("SET", "other", "x", "XX"));
        assertEquals("OK", call("SET", "k", "3", "XX"));
        assertEquals("3", call("GET", "k"));
    }

    @Test
    void setGetReturnsTheOldValue() {
        assertNull(call("SET", "k", "first", "GET"));
        assertEquals("first", call("SET", "k", "second", "GET"));
        assertEquals("second", call("GET", "k"));
    }

    @Test
    void setRejectsBadOptions() {
        assertError("ERR syntax error", call("SET", "k", "v", "NX", "XX"));
        assertError("ERR syntax error", call("SET", "k", "v", "EX"));
        assertError("ERR syntax error", call("SET", "k", "v", "BOGUS"));
        assertError("ERR invalid expire time", call("SET", "k", "v", "EX", "0"));
        assertError("ERR value is not an integer", call("SET", "k", "v", "EX", "ten"));
    }

    @Test
    void keysExpireExactlyOnTime() {
        call("SET", "session", "abc", "PX", "100");
        clock.addAndGet(99);
        assertEquals("abc", call("GET", "session"));
        clock.addAndGet(1);
        assertNull(call("GET", "session"));
        assertEquals(0L, call("EXISTS", "session"));
    }

    @Test
    void ttlPersistAndExpire() {
        assertEquals(-2L, call("TTL", "k"));
        assertEquals(0L, call("EXPIRE", "k", "10"));
        call("SET", "k", "v");
        assertEquals(-1L, call("TTL", "k"));
        assertEquals(1L, call("EXPIRE", "k", "10"));
        assertEquals(10L, call("TTL", "k"));
        assertEquals(10_000L, call("PTTL", "k"));
        assertEquals(1L, call("PERSIST", "k"));
        assertEquals(-1L, call("TTL", "k"));
        assertEquals(0L, call("PERSIST", "k"));
        assertEquals(1L, call("EXPIRE", "k", "-1"));
        assertEquals(0L, call("EXISTS", "k"));
    }

    @Test
    void setClearsTheTtlButIncrAndAppendKeepIt() {
        call("SET", "n", "1", "EX", "100");
        call("INCR", "n");
        call("APPEND", "n", "0");
        assertEquals(100L, call("TTL", "n"));
        call("SET", "n", "5");
        assertEquals(-1L, call("TTL", "n"));
    }

    @Test
    void incrementAndDecrement() {
        assertEquals(1L, call("INCR", "counter"));
        assertEquals(11L, call("INCRBY", "counter", "10"));
        assertEquals(10L, call("DECR", "counter"));
        assertEquals(-10L, call("DECRBY", "counter", "20"));
        assertEquals("-10", call("GET", "counter"));
    }

    @Test
    void incrRejectsValuesThatAreNotStrictIntegers() {
        for (String value : List.of("abc", "+5", " 5", "05", "1.5", "", "-0")) {
            call("SET", "n", value);
            assertError("ERR value is not an integer", call("INCR", "n"));
        }
    }

    @Test
    void incrDetectsOverflow() {
        call("SET", "n", String.valueOf(Long.MAX_VALUE));
        assertError("ERR increment or decrement would overflow", call("INCR", "n"));
        call("SET", "n", String.valueOf(Long.MIN_VALUE));
        assertError("ERR increment or decrement would overflow", call("DECR", "n"));
        assertError("ERR decrement would overflow", call("DECRBY", "x", String.valueOf(Long.MIN_VALUE)));
    }

    @Test
    void appendStrlenMsetMget() {
        assertEquals(5L, call("APPEND", "s", "hello"));
        assertEquals(11L, call("APPEND", "s", " world"));
        assertEquals(11L, call("STRLEN", "s"));
        assertEquals(0L, call("STRLEN", "missing"));
        assertEquals("OK", call("MSET", "a", "1", "b", "2"));
        call("RPUSH", "list", "x");
        assertEquals(Arrays.asList("1", "2", null, null), call("MGET", "a", "b", "missing", "list"));
        assertError("ERR wrong number of arguments", call("MSET", "a", "1", "b"));
    }

    @Test
    void delExistsTypeAndDbsize() {
        call("MSET", "a", "1", "b", "2");
        call("RPUSH", "l", "x");
        assertEquals(3L, call("DBSIZE"));
        assertEquals("string", call("TYPE", "a"));
        assertEquals("list", call("TYPE", "l"));
        assertEquals("none", call("TYPE", "zzz"));
        assertEquals(3L, call("EXISTS", "a", "a", "l"));
        assertEquals(2L, call("DEL", "a", "l", "missing"));
        assertEquals(1L, call("DBSIZE"));
        assertEquals("OK", call("FLUSHALL"));
        assertEquals(0L, call("DBSIZE"));
    }

    @Test
    void keysMatchesGlobPatternsAndSkipsExpiredKeys() {
        call("MSET", "user:1", "a", "user:2", "b", "order:1", "c");
        call("SET", "user:3", "gone", "PX", "10");
        clock.addAndGet(10);
        @SuppressWarnings("unchecked")
        List<String> keys = new ArrayList<>((List<String>) call("KEYS", "user:*"));
        keys.sort(null);
        assertEquals(List.of("user:1", "user:2"), keys);
    }

    @Test
    void lists() {
        assertEquals(3L, call("RPUSH", "l", "a", "b", "c"));
        assertEquals(4L, call("LPUSH", "l", "z"));
        assertEquals(List.of("z", "a", "b", "c"), call("LRANGE", "l", "0", "-1"));
        assertEquals(List.of("b", "c"), call("LRANGE", "l", "-2", "-1"));
        assertEquals(List.of("z", "a"), call("LRANGE", "l", "-100", "1"));
        assertEquals(List.of(), call("LRANGE", "l", "5", "10"));
        assertEquals("z", call("LPOP", "l"));
        assertEquals(List.of("c", "b"), call("RPOP", "l", "2"));
        assertEquals(1L, call("LLEN", "l"));
        assertEquals("a", call("LPOP", "l"));
        assertEquals(0L, call("EXISTS", "l"), "an emptied list is deleted");
        assertNull(call("LPOP", "l"));
        assertNull(call("LPOP", "l", "3"));
    }

    @Test
    void wrongTypeErrors() {
        call("RPUSH", "list", "a");
        call("SET", "string", "v");
        assertError("WRONGTYPE", call("GET", "list"));
        assertError("WRONGTYPE", call("INCR", "list"));
        assertError("WRONGTYPE", call("APPEND", "list", "x"));
        assertError("WRONGTYPE", call("SET", "list", "v", "GET"));
        assertError("WRONGTYPE", call("LPUSH", "string", "x"));
        assertError("WRONGTYPE", call("LRANGE", "string", "0", "-1"));
    }

    @Test
    void unknownCommandsAndWrongArgumentCounts() {
        assertError("ERR unknown command 'nope'", call("NOPE"));
        assertError("ERR wrong number of arguments for 'get' command", call("GET"));
        assertError("ERR wrong number of arguments for 'get' command", call("GET", "a", "b"));
        assertEquals("OK", call("set", "lower", "case"), "command names are case-insensitive");
    }

    @Test
    void writesArePropagatedWithAbsoluteExpiryTimes() {
        call("SET", "k", "v", "EX", "10");
        call("GET", "k");
        call("SET", "k", "v2", "NX");
        call("EXPIRE", "k", "60");
        call("INCR", "n");
        call("DEL", "missing");
        call("EXPIRE", "n", "-1");
        assertEquals(List.of(
                "SET k v",
                "PEXPIREAT k 1010000",
                "PEXPIREAT k 1060000",
                "SET n 1 KEEPTTL",
                "DEL n"), propagated);
    }

    @Test
    void activeExpiryRemovesKeysNobodyReads() {
        for (int i = 0; i < 1_000; i++) {
            store.put("temp:" + i, bytes("x"), false);
            store.expireAt("temp:" + i, clock.get() + 10);
            store.put("keep:" + i, bytes("x"), false);
        }
        clock.addAndGet(10);
        int removed = 0;
        while (store.expiringCount() > 0) {
            removed += store.expireCycle(Long.MAX_VALUE);
        }
        assertEquals(1_000, removed);
        assertEquals(1_000, store.size());
        assertTrue(store.keys("keep:*").size() == 1_000);
    }

    @Test
    void strictLongParsing() {
        assertEquals(0L, CommandProcessor.parseStrictLong(bytes("0")));
        assertEquals(Long.MAX_VALUE, CommandProcessor.parseStrictLong(bytes("9223372036854775807")));
        assertEquals(Long.MIN_VALUE, CommandProcessor.parseStrictLong(bytes("-9223372036854775808")));
        assertNull(CommandProcessor.parseStrictLong(bytes("9223372036854775808")));
        assertNull(CommandProcessor.parseStrictLong(bytes("-")));
    }
}
