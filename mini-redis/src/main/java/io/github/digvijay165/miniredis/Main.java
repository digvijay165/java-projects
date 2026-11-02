package io.github.digvijay165.miniredis;

import io.github.digvijay165.miniredis.bench.Benchmark;
import io.github.digvijay165.miniredis.command.CommandProcessor;
import io.github.digvijay165.miniredis.persistence.AppendOnlyFile;
import io.github.digvijay165.miniredis.server.RedisServer;
import io.github.digvijay165.miniredis.store.DataStore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Arrays;

public final class Main {
    private static final String USAGE = """
            Usage:
              java -jar mini-redis.jar [--port 6379] [--bind 127.0.0.1] [--aof appendonly.aof | --no-aof]
              java -jar mini-redis.jar bench [--host 127.0.0.1] [--port 6379] [--clients 50]
                                             [--requests 100000] [--pipeline 1] [--size 16]
            """;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("bench")) {
            Benchmark.run(Arrays.copyOfRange(args, 1, args.length));
            return;
        }

        int port = 6379;
        String bind = "127.0.0.1";
        String aofPath = "appendonly.aof";
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(value(args, ++i));
                case "--bind" -> bind = value(args, ++i);
                case "--aof" -> aofPath = value(args, ++i);
                case "--no-aof" -> aofPath = null;
                case "--help", "-h" -> {
                    System.out.print(USAGE);
                    return;
                }
                default -> exitWithUsage("Unknown option: " + args[i]);
            }
        }

        DataStore store = new DataStore();
        CommandProcessor processor = new CommandProcessor(store);
        AppendOnlyFile aof = null;
        if (aofPath != null) {
            aof = new AppendOnlyFile(Path.of(aofPath));
            long started = System.nanoTime();
            int replayed = aof.replay(processor);
            Log.info(String.format("Loaded %d keys from %s (%d commands replayed in %d ms)",
                    store.size(), aofPath, replayed, (System.nanoTime() - started) / 1_000_000));
            aof.open();
            AppendOnlyFile file = aof;
            processor.setPropagation(file::append);
            processor.setAofRewriter(() -> file.rewrite(store));
        } else {
            Log.info("Persistence is off: data lives in memory only");
        }

        RedisServer server = new RedisServer(new InetSocketAddress(bind, port), store, processor, aof);
        int boundPort = server.start();
        Log.info("mini-redis is ready for connections on " + bind + ":" + boundPort);

        AppendOnlyFile aofToClose = aof;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Log.info("Shutting down");
            server.close();
            if (aofToClose != null) {
                try {
                    aofToClose.close();
                } catch (IOException e) {
                    Log.error("Closing the append only file failed", e);
                }
            }
        }, "mini-redis-shutdown"));
        server.awaitTermination();
    }

    private static String value(String[] args, int i) {
        if (i >= args.length) {
            exitWithUsage("Missing value for " + args[i - 1]);
        }
        return args[i];
    }

    private static void exitWithUsage(String message) {
        System.err.println(message);
        System.err.print(USAGE);
        System.exit(2);
    }
}
