package io.github.digvijay165.miniredis.server;

import io.github.digvijay165.miniredis.Replies;
import io.github.digvijay165.miniredis.command.CommandProcessor;
import io.github.digvijay165.miniredis.protocol.Bytes;
import io.github.digvijay165.miniredis.protocol.Resp;
import io.github.digvijay165.miniredis.protocol.RespReader;
import io.github.digvijay165.miniredis.store.DataStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.github.digvijay165.miniredis.Replies.assertError;
import static io.github.digvijay165.miniredis.protocol.Bytes.bytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Starts the real server on a random port and talks to it over TCP. */
@Timeout(30)
class RedisServerTest {
    private RedisServer server;
    private int port;

    @BeforeEach
    void startServer() throws IOException {
        DataStore store = new DataStore();
        server = new RedisServer(new InetSocketAddress("127.0.0.1", 0), store, new CommandProcessor(store), null);
        port = server.start();
    }

    @AfterEach
    void stopServer() {
        server.close();
    }

    /** A minimal blocking client. */
    private final class Connection implements Closeable {
        final Socket socket = new Socket("127.0.0.1", port);
        final OutputStream out = new BufferedOutputStream(socket.getOutputStream());
        final RespReader in = new RespReader(socket.getInputStream());

        Connection() throws IOException {
            socket.setSoTimeout(10_000);
        }

        void send(String... args) throws IOException {
            sendRaw(Resp.array(Arrays.stream(args).map(Bytes::bytes).toList()));
        }

        void sendRaw(byte[] data) throws IOException {
            out.write(data);
            out.flush();
        }

        Object read() throws IOException {
            return Replies.decode(in.read());
        }

        Object call(String... args) throws IOException {
            send(args);
            return read();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    @Test
    void answersCommandsOverTcp() throws IOException {
        try (Connection redis = new Connection()) {
            assertEquals("PONG", redis.call("PING"));
            assertEquals("OK", redis.call("SET", "greeting", "hello"));
            assertEquals("hello", redis.call("GET", "greeting"));
        }
    }

    @Test
    void handlesTenThousandPipelinedCommands() throws IOException {
        try (Connection redis = new Connection()) {
            for (int i = 0; i < 10_000; i++) {
                redis.out.write(Resp.array(List.of(bytes("INCR"), bytes("hits"))));
            }
            redis.out.flush();
            for (long expected = 1; expected <= 10_000; expected++) {
                assertEquals(expected, redis.read());
            }
        }
    }

    @Test
    void assemblesCommandsSentOneByteAtATime() throws IOException {
        try (Connection redis = new Connection()) {
            redis.socket.setTcpNoDelay(true);
            for (byte b : Resp.array(List.of(bytes("SET"), bytes("slow"), bytes("drip")))) {
                redis.sendRaw(new byte[] {b});
            }
            assertEquals("OK", redis.read());
            assertEquals("drip", redis.call("GET", "slow"));
        }
    }

    @Test
    void largeAndBinaryValuesRoundTrip() throws IOException {
        byte[] big = new byte[5 * 1024 * 1024];
        new Random(42).nextBytes(big);
        try (Connection redis = new Connection()) {
            redis.sendRaw(Resp.array(List.of(bytes("SET"), bytes("blob"), big)));
            assertEquals("OK", redis.read());
            redis.send("GET", "blob");
            assertArrayEquals(big, (byte[]) redis.in.read());
        }
    }

    @Test
    void incrementsFromManyClientsAreAtomic() throws Exception {
        int clients = 20;
        int perClient = 500;
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        try {
            List<Future<?>> results = new ArrayList<>();
            for (int i = 0; i < clients; i++) {
                results.add(pool.submit(() -> {
                    try (Connection redis = new Connection()) {
                        for (int j = 0; j < perClient; j++) {
                            redis.call("INCR", "shared");
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> result : results) {
                result.get();
            }
        } finally {
            pool.shutdownNow();
        }
        try (Connection redis = new Connection()) {
            assertEquals(String.valueOf(clients * perClient), redis.call("GET", "shared"));
        }
    }

    @Test
    void inlineCommandsWorkLikeTelnet() throws IOException {
        try (Connection redis = new Connection()) {
            redis.sendRaw(bytes("SET city Kota\r\nGET city\r\n"));
            assertEquals("OK", redis.read());
            assertEquals("Kota", redis.read());
        }
    }

    @Test
    void keysExpireOnTheServer() throws Exception {
        try (Connection redis = new Connection()) {
            redis.call("SET", "flash", "now you see me", "PX", "50");
            Thread.sleep(120);
            assertNull(redis.call("GET", "flash"));
        }
    }

    @Test
    void aProtocolErrorIsReportedAndTheConnectionClosed() throws IOException {
        try (Connection redis = new Connection()) {
            redis.sendRaw(bytes("*1\r\n+oops\r\n"));
            assertError("ERR Protocol error: expected '$'", redis.read());
            assertThrows(IOException.class, redis::read);
        }
    }

    @Test
    void quitSaysOkAndCloses() throws IOException {
        try (Connection redis = new Connection()) {
            assertEquals("OK", redis.call("QUIT"));
            assertThrows(IOException.class, redis::read);
        }
    }

    @Test
    void oneClientsErrorDoesNotAffectAnother() throws IOException {
        try (Connection good = new Connection(); Connection bad = new Connection()) {
            good.call("SET", "k", "v");
            bad.sendRaw(bytes("*1\r\n$99999999999\r\n"));
            assertError("ERR Protocol error", bad.read());
            assertEquals("v", good.call("GET", "k"));
        }
    }
}
