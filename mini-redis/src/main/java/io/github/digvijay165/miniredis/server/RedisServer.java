package io.github.digvijay165.miniredis.server;

import io.github.digvijay165.miniredis.Log;
import io.github.digvijay165.miniredis.command.CommandProcessor;
import io.github.digvijay165.miniredis.persistence.AppendOnlyFile;
import io.github.digvijay165.miniredis.protocol.Bytes;
import io.github.digvijay165.miniredis.protocol.ProtocolException;
import io.github.digvijay165.miniredis.protocol.Resp;
import io.github.digvijay165.miniredis.protocol.RespParser;
import io.github.digvijay165.miniredis.store.DataStore;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;

/**
 * A single-threaded, non-blocking TCP server, built the way Redis itself is.
 *
 * <p>One event loop thread owns all the data. It waits on a {@link Selector} for sockets that are
 * readable or writable, parses every complete command a client has sent (clients may pipeline
 * many at once), runs them one after another and queues the replies. Because only one thread
 * ever touches the data, every command is atomic and nothing needs a lock.
 *
 * <p>Between events, a "cron" pass every 100 ms deletes expired keys, fsyncs the append-only
 * file once a second and rewrites it when it grows too large.
 */
public final class RedisServer implements Closeable {
    private static final int READ_CHUNK = 16 * 1024;
    /** Redis's default client-query-buffer-limit. */
    private static final int MAX_QUERY_BUFFER = 1024 * 1024 * 1024;
    /** Stop reading from a client whose unsent replies pass this, until it catches up. */
    private static final int MAX_PENDING_OUTPUT = 64 * 1024 * 1024;
    private static final long CRON_INTERVAL_NANOS = 100_000_000L;
    private static final long EXPIRE_BUDGET_NANOS = 25_000_000L;
    private static final long FSYNC_INTERVAL_NANOS = 1_000_000_000L;

    private final InetSocketAddress address;
    private final DataStore store;
    private final CommandProcessor processor;
    private final AppendOnlyFile aof;
    private volatile Selector selector;
    private ServerSocketChannel serverChannel;
    private volatile Thread loop;
    private volatile boolean running;

    /** aof may be null to run without persistence. */
    public RedisServer(InetSocketAddress address, DataStore store, CommandProcessor processor, AppendOnlyFile aof) {
        this.address = address;
        this.store = store;
        this.processor = processor;
        this.aof = aof;
    }

    /** Binds the port and starts the event loop thread. Returns the bound port, which helps when asking for port 0. */
    public int start() throws IOException {
        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverChannel.bind(address, 511);
        serverChannel.configureBlocking(false);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        running = true;
        loop = new Thread(this::eventLoop, "mini-redis-event-loop");
        loop.start();
        return ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();
    }

    public void awaitTermination() throws InterruptedException {
        loop.join();
    }

    /** Stops the event loop and closes every connection. */
    @Override
    public void close() {
        running = false;
        Selector s = selector;
        if (s != null) {
            s.wakeup();
        }
        Thread t = loop;
        if (t != null && t != Thread.currentThread()) {
            try {
                t.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void eventLoop() {
        long nextCron = System.nanoTime() + CRON_INTERVAL_NANOS;
        long nextFsync = System.nanoTime() + FSYNC_INTERVAL_NANOS;
        try {
            while (running) {
                long waitMillis = Math.max(1, (nextCron - System.nanoTime()) / 1_000_000);
                selector.select(waitMillis);
                Iterator<SelectionKey> selected = selector.selectedKeys().iterator();
                while (selected.hasNext()) {
                    SelectionKey key = selected.next();
                    selected.remove();
                    handle(key);
                }

                long now = System.nanoTime();
                if (now - nextCron >= 0) {
                    store.expireCycle(EXPIRE_BUDGET_NANOS);
                    boolean fsync = now - nextFsync >= 0;
                    maintainAof(fsync);
                    if (fsync) {
                        nextFsync = now + FSYNC_INTERVAL_NANOS;
                    }
                    nextCron = now + CRON_INTERVAL_NANOS;
                }
            }
        } catch (IOException | RuntimeException e) {
            Log.error("Event loop stopped", e);
        } finally {
            for (SelectionKey key : selector.keys()) {
                closeQuietly(key.channel());
            }
            closeQuietly(selector);
        }
    }

    private void handle(SelectionKey key) {
        try {
            if (key.isAcceptable()) {
                accept();
                return;
            }
            Client client = (Client) key.attachment();
            if (key.isReadable()) {
                read(key, client);
            }
            if (key.isValid() && key.isWritable()) {
                serve(key, client);
            }
        } catch (IOException | CancelledKeyException e) {
            if (key.channel() == serverChannel) {
                Log.error("Accepting a connection failed", e);
            } else {
                disconnect(key);
            }
        }
    }

    private void accept() throws IOException {
        SocketChannel channel;
        while ((channel = serverChannel.accept()) != null) {
            channel.configureBlocking(false);
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            channel.register(selector, SelectionKey.OP_READ, new Client());
        }
    }

    private void read(SelectionKey key, Client client) throws IOException {
        client.in.ensureWritable(READ_CHUNK);
        int n = ((SocketChannel) key.channel()).read(client.in.writableBuffer());
        if (n < 0) {
            disconnect(key);
            return;
        }
        client.in.written(n);
        if (client.in.readable() > MAX_QUERY_BUFFER) {
            client.out.append(Resp.error("ERR Protocol error: query buffer limit exceeded"));
            client.closeAfterWrite = true;
        }
        serve(key, client);
    }

    /** Runs the client's buffered commands and sends the replies, then decides what to wait for next. */
    private void serve(SelectionKey key, Client client) throws IOException {
        boolean paused;
        do {
            paused = runCommands(client);
            flushAof();
            writeReplies(key, client);
        } while (paused && client.out.readable() == 0);

        if (client.closeAfterWrite && client.out.readable() == 0) {
            disconnect(key);
            return;
        }
        int ops = client.out.readable() > 0 ? SelectionKey.OP_WRITE : 0;
        if (!client.closeAfterWrite && client.out.readable() <= MAX_PENDING_OUTPUT) {
            ops |= SelectionKey.OP_READ;
        }
        key.interestOps(ops);
    }

    /**
     * Runs every complete command in the client's input buffer.
     *
     * @return true if it stopped early because too many replies are waiting to be sent
     */
    private boolean runCommands(Client client) {
        while (!client.closeAfterWrite) {
            if (client.out.readable() > MAX_PENDING_OUTPUT) {
                return true;
            }
            RespParser.Command command;
            try {
                command = RespParser.parse(client.in.array(), client.in.start(), client.in.end());
            } catch (ProtocolException e) {
                client.out.append(Resp.error("ERR Protocol error: " + e.getMessage()));
                client.closeAfterWrite = true;
                return false;
            }
            if (command == null) {
                return false;
            }
            client.in.consume(command.length());

            List<byte[]> args = command.args();
            if (args.isEmpty()) {
                continue;
            }
            if (Bytes.str(args.get(0)).equalsIgnoreCase("quit")) {
                client.out.append(Resp.OK);
                client.closeAfterWrite = true;
                return false;
            }
            byte[] reply;
            try {
                reply = processor.execute(args);
            } catch (RuntimeException e) {
                Log.error("Command failed", e);
                reply = Resp.error("ERR internal error: " + e.getMessage());
            }
            client.out.append(reply);
        }
        return false;
    }

    /** Writes this pass's commands to the AOF before any reply goes out. */
    private void flushAof() {
        if (aof == null) {
            return;
        }
        try {
            aof.flush();
        } catch (IOException e) {
            Log.error("Writing the append only file failed", e);
        }
    }

    private void maintainAof(boolean fsync) {
        if (aof == null) {
            return;
        }
        try {
            if (aof.shouldRewrite()) {
                long before = aof.size();
                aof.rewrite(store);
                Log.info("Rewrote the append only file: " + before + " -> " + aof.size() + " bytes");
            }
            if (fsync) {
                aof.sync();
            }
        } catch (IOException e) {
            Log.error("Append only file maintenance failed", e);
        }
    }

    private static void writeReplies(SelectionKey key, Client client) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        while (client.out.readable() > 0) {
            int n = channel.write(client.out.readableBuffer());
            if (n == 0) {
                return; // socket buffer full; OP_WRITE will tell us when to continue
            }
            client.out.consume(n);
        }
    }

    private static void disconnect(SelectionKey key) {
        key.cancel();
        closeQuietly(key.channel());
    }

    private static void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException ignored) {
            // Nothing useful to do; the connection is going away anyway.
        }
    }
}
