package io.github.digvijay165.miniredis.persistence;

import io.github.digvijay165.miniredis.command.CommandProcessor;
import io.github.digvijay165.miniredis.protocol.Bytes;
import io.github.digvijay165.miniredis.store.DataStore;
import io.github.digvijay165.miniredis.store.RedisList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.digvijay165.miniredis.protocol.Bytes.bytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AppendOnlyFileTest {
    @TempDir
    Path dir;

    private final AtomicLong clock = new AtomicLong(1_000_000);

    /** Starts a "server": replays the file into a fresh store, then logs new writes to it. */
    private CommandProcessor start(DataStore store, AppendOnlyFile aof) throws IOException {
        CommandProcessor processor = new CommandProcessor(store);
        aof.replay(processor);
        aof.open();
        processor.setPropagation(aof::append);
        return processor;
    }

    private static void run(CommandProcessor processor, String... args) {
        byte[] reply = processor.execute(Arrays.stream(args).map(Bytes::bytes).toList());
        assertTrue(reply[0] != '-', () -> "command failed: " + Bytes.str(reply));
    }

    private DataStore restart(Path file) throws IOException {
        DataStore store = new DataStore(clock::get);
        new AppendOnlyFile(file).replay(new CommandProcessor(store));
        return store;
    }

    private static String string(DataStore store, String key) {
        DataStore.Entry entry = store.get(key);
        return entry == null ? null : Bytes.str((byte[]) entry.value());
    }

    @Test
    void dataSurvivesARestart() throws IOException {
        Path file = dir.resolve("appendonly.aof");
        try (AppendOnlyFile aof = new AppendOnlyFile(file)) {
            CommandProcessor redis = start(new DataStore(clock::get), aof);
            run(redis, "SET", "name", "digvijay");
            run(redis, "RPUSH", "langs", "java", "c++");
            run(redis, "INCRBY", "visits", "41");
            run(redis, "INCR", "visits");
            run(redis, "SET", "temp", "x");
            run(redis, "DEL", "temp");
        }

        DataStore store = restart(file);
        assertEquals("digvijay", string(store, "name"));
        assertEquals("42", string(store, "visits"));
        assertNull(store.get("temp"));
        RedisList langs = (RedisList) store.get("langs").value();
        assertEquals(List.of("java", "c++"), langs.stream().map(Bytes::str).toList());
    }

    @Test
    void expiryTimesAreKeptAcrossRestarts() throws IOException {
        Path file = dir.resolve("appendonly.aof");
        try (AppendOnlyFile aof = new AppendOnlyFile(file)) {
            CommandProcessor redis = start(new DataStore(clock::get), aof);
            run(redis, "SET", "session", "abc", "EX", "10");
        }

        clock.addAndGet(4_000); // restart 4 seconds later: 6 seconds should be left, not a fresh 10
        DataStore store = restart(file);
        assertEquals(1_000_000 + 10_000, store.get("session").expireAt());

        clock.addAndGet(10_000); // restart after the deadline: the key is gone
        assertNull(restart(file).get("session"));
    }

    @Test
    void aHalfWrittenCommandAtTheEndIsTrimmed() throws IOException {
        Path file = dir.resolve("appendonly.aof");
        try (AppendOnlyFile aof = new AppendOnlyFile(file)) {
            CommandProcessor redis = start(new DataStore(clock::get), aof);
            run(redis, "SET", "a", "1");
            run(redis, "SET", "b", "2");
        }
        long goodSize = Files.size(file);
        Files.write(file, bytes("*3\r\n$3\r\nSET\r\n$1\r\nc"), StandardOpenOption.APPEND); // crash mid-write

        DataStore store = restart(file);
        assertEquals("1", string(store, "a"));
        assertEquals("2", string(store, "b"));
        assertNull(store.get("c"));
        assertEquals(goodSize, Files.size(file));
    }

    @Test
    void aCorruptedFileFailsLoudly() throws IOException {
        Path file = dir.resolve("appendonly.aof");
        Files.write(file, bytes("*1\r\n+not a bulk string\r\n"));
        IOException error = assertThrows(IOException.class, () -> restart(file));
        assertTrue(error.getMessage().contains("corrupted at byte 0"));
    }

    @Test
    void rewriteShrinksTheFileAndKeepsTheData() throws IOException {
        Path file = dir.resolve("appendonly.aof");
        DataStore store = new DataStore(clock::get);
        byte[] binary = {0, '\r', '\n', (byte) 0xff};
        try (AppendOnlyFile aof = new AppendOnlyFile(file)) {
            CommandProcessor redis = start(store, aof);
            for (int i = 0; i < 1_000; i++) {
                run(redis, "INCR", "counter");
            }
            for (int i = 0; i < 200; i++) {
                run(redis, "RPUSH", "list", "item" + i);
            }
            run(redis, "SET", "session", "abc", "EX", "100");
            redis.execute(List.of(bytes("SET"), bytes("binary"), binary));
            aof.flush();
            long before = aof.size();

            aof.rewrite(store);
            assertTrue(aof.size() < before / 5, "rewrite should shrink " + before + " bytes, got " + aof.size());
            run(redis, "INCR", "counter"); // writes after a rewrite go to the new file
        }

        DataStore restored = restart(file);
        assertEquals("1001", string(restored, "counter"));
        assertEquals(200, ((RedisList) restored.get("list").value()).size());
        assertEquals(store.get("session").expireAt(), restored.get("session").expireAt());
        assertArrayEquals(binary, (byte[]) restored.get("binary").value());
    }
}
