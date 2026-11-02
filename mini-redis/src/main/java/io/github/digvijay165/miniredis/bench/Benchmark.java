package io.github.digvijay165.miniredis.bench;

import io.github.digvijay165.miniredis.protocol.Resp;
import io.github.digvijay165.miniredis.protocol.RespReader;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import static io.github.digvijay165.miniredis.protocol.Bytes.bytes;

/**
 * A load generator in the spirit of redis-benchmark. Many clients connect at once and each sends
 * its share of the requests, optionally pipelined, while every round trip's latency is recorded.
 * It works against real Redis too, which makes a fair comparison easy.
 */
public final class Benchmark {
    private record Config(String host, int port, int clients, int requests, int pipeline, int size) {
    }

    private static final int KEY_SPACE = 100_000;

    private Benchmark() {
    }

    public static void run(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = 6379;
        int clients = 50;
        int requests = 100_000;
        int pipeline = 1;
        int size = 16;
        for (int i = 0; i < args.length; i += 2) {
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("missing value for " + args[i]);
            }
            String value = args[i + 1];
            switch (args[i]) {
                case "--host" -> host = value;
                case "--port" -> port = Integer.parseInt(value);
                case "--clients" -> clients = Integer.parseInt(value);
                case "--requests" -> requests = Integer.parseInt(value);
                case "--pipeline" -> pipeline = Integer.parseInt(value);
                case "--size" -> size = Integer.parseInt(value);
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        Config config = new Config(host, port, clients, requests, pipeline, size);

        byte[] value = new byte[size];
        Arrays.fill(value, (byte) 'x');
        System.out.printf("%d clients, %,d requests per test, %d-byte values, pipeline %d%n",
                clients, requests, size, pipeline);
        if (pipeline > 1) {
            System.out.println("(latency is per pipelined batch)");
        }
        System.out.println();

        test("PING", config, random -> List.of(bytes("PING")));
        test("SET", config, random -> List.of(bytes("SET"), key(random), value));
        test("GET", config, random -> List.of(bytes("GET"), key(random)));
        test("INCR", config, random -> List.of(bytes("INCR"), bytes("counter:" + random.nextInt(1_000))));
        test("LPUSH", config, random -> List.of(bytes("LPUSH"), bytes("bench:list"), value));
        test("LPOP", config, random -> List.of(bytes("LPOP"), bytes("bench:list")));
    }

    private static byte[] key(SplittableRandom random) {
        return bytes("key:" + random.nextInt(KEY_SPACE));
    }

    private static void test(String name, Config config, Function<SplittableRandom, List<byte[]>> request) throws Exception {
        int perClient = Math.max(1, config.requests() / config.clients());
        int batches = (perClient + config.pipeline() - 1) / config.pipeline();
        long[][] latencies = new long[config.clients()][];
        CountDownLatch connected = new CountDownLatch(config.clients());
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(config.clients());
        List<Future<?>> results = new ArrayList<>();

        for (int id = 0; id < config.clients(); id++) {
            int clientId = id;
            results.add(pool.submit(() -> {
                try (Socket socket = connect(config, connected)) {
                    socket.setTcpNoDelay(true);
                    OutputStream out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
                    RespReader in = new RespReader(socket.getInputStream());
                    SplittableRandom random = new SplittableRandom(clientId);
                    long[] samples = new long[batches];
                    go.await();
                    int sent = 0;
                    for (int batch = 0; batch < batches; batch++) {
                        int count = Math.min(config.pipeline(), perClient - sent);
                        long start = System.nanoTime();
                        for (int i = 0; i < count; i++) {
                            out.write(Resp.array(request.apply(random)));
                        }
                        out.flush();
                        for (int i = 0; i < count; i++) {
                            if (in.read() instanceof RespReader.ErrorReply error) {
                                throw new IOException(name + " failed: " + error.message());
                            }
                        }
                        samples[batch] = System.nanoTime() - start;
                        sent += count;
                    }
                    latencies[clientId] = samples;
                }
                return null;
            }));
        }

        connected.await();
        long start = System.nanoTime();
        go.countDown();
        try {
            for (Future<?> result : results) {
                result.get();
            }
        } finally {
            pool.shutdownNow();
        }
        long elapsed = System.nanoTime() - start;

        long[] all = Arrays.stream(latencies).flatMapToLong(Arrays::stream).sorted().toArray();
        double perSecond = (double) perClient * config.clients() / (elapsed / 1e9);
        System.out.printf("%-6s %,10.0f requests/sec   p50 %6.3f ms   p99 %6.3f ms   max %7.3f ms%n",
                name, perSecond, percentile(all, 50), percentile(all, 99), all[all.length - 1] / 1e6);
    }

    /** Connects, and counts down the latch either way so a failed connection cannot hang the run. */
    private static Socket connect(Config config, CountDownLatch connected) throws IOException {
        try {
            return new Socket(config.host(), config.port());
        } finally {
            connected.countDown();
        }
    }

    private static double percentile(long[] sorted, double p) {
        int index = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(index, sorted.length - 1))] / 1e6;
    }
}
