package io.github.digvijay165.miniredis.persistence;

import io.github.digvijay165.miniredis.Log;
import io.github.digvijay165.miniredis.command.CommandProcessor;
import io.github.digvijay165.miniredis.protocol.ProtocolException;
import io.github.digvijay165.miniredis.protocol.Resp;
import io.github.digvijay165.miniredis.protocol.RespParser;
import io.github.digvijay165.miniredis.store.DataStore;
import io.github.digvijay165.miniredis.store.RedisList;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import static io.github.digvijay165.miniredis.protocol.Bytes.bytes;
import static io.github.digvijay165.miniredis.protocol.Bytes.str;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Append-only file persistence, the same scheme as Redis's AOF.
 *
 * <p>Every write command is appended to the file in RESP form. Commands are buffered during one
 * pass of the event loop and written before that pass sends its replies, so a client is never
 * told "OK" for a write the file has not received. The file is fsynced once a second (Redis's
 * "everysec" policy), so a power cut loses at most about a second of writes.
 *
 * <p>On start-up the file is replayed to rebuild the data. When it has doubled in size since the
 * last rewrite (and is at least 64 MB), it is rewritten as the shortest list of commands that
 * recreates the current data: a counter incremented a million times becomes a single SET.
 */
public final class AppendOnlyFile implements Closeable {
    private static final long MIN_REWRITE_SIZE = 64L * 1024 * 1024;
    private static final int LIST_BATCH = 64;
    private static final byte[] SET = bytes("SET");
    private static final byte[] RPUSH = bytes("RPUSH");
    private static final byte[] PEXPIREAT = bytes("PEXPIREAT");

    private final Path path;
    private final ByteArrayOutputStream pending = new ByteArrayOutputStream();
    private FileChannel channel;
    private long size;
    private long sizeAfterRewrite;
    private boolean dirty;

    public AppendOnlyFile(Path path) {
        this.path = path;
    }

    /**
     * Replays the file through the processor. A command cut off halfway by a crash is trimmed from
     * the end of the file; anything else that is not valid fails loudly instead of losing data.
     *
     * @return how many commands were replayed
     */
    public int replay(CommandProcessor processor) throws IOException {
        if (!Files.exists(path)) {
            return 0;
        }
        byte[] data = Files.readAllBytes(path);
        int position = 0;
        int replayed = 0;
        while (position < data.length) {
            RespParser.Command command;
            try {
                command = RespParser.parse(data, position, data.length);
            } catch (ProtocolException e) {
                throw new IOException("AOF " + path + " is corrupted at byte " + position + ": " + e.getMessage(), e);
            }
            if (command == null) {
                Log.warn("AOF ends with a half-written command; trimming the last " + (data.length - position) + " bytes");
                try (FileChannel file = FileChannel.open(path, WRITE)) {
                    file.truncate(position);
                }
                break;
            }
            if (!command.args().isEmpty()) {
                byte[] reply = processor.execute(command.args());
                if (reply[0] == '-') {
                    throw new IOException("AOF command at byte " + position + " failed: " + str(reply).trim());
                }
                replayed++;
            }
            position += command.length();
        }
        return replayed;
    }

    /** Opens the file for appending, creating it if needed. Call after {@link #replay}. */
    public void open() throws IOException {
        channel = FileChannel.open(path, CREATE, WRITE, APPEND);
        size = channel.size();
        sizeAfterRewrite = size;
    }

    /** Buffers a write command. It reaches the file on the next {@link #flush}. */
    public void append(List<byte[]> command) {
        pending.writeBytes(Resp.array(command));
    }

    /** Writes buffered commands to the file. They are with the OS now, but not yet on disk. */
    public void flush() throws IOException {
        if (pending.size() == 0) {
            return;
        }
        ByteBuffer buffer = ByteBuffer.wrap(pending.toByteArray());
        pending.reset();
        while (buffer.hasRemaining()) {
            size += channel.write(buffer);
        }
        dirty = true;
    }

    /** Flushes and then forces everything written so far onto the disk. */
    public void sync() throws IOException {
        flush();
        if (dirty) {
            channel.force(false);
            dirty = false;
        }
    }

    public long size() {
        return size;
    }

    public boolean shouldRewrite() {
        return size >= MIN_REWRITE_SIZE && size >= 2 * sizeAfterRewrite;
    }

    /**
     * Replaces the file with a compact snapshot of the store. The snapshot goes to a temporary
     * file that is fsynced and then renamed over the old one, so a crash at any point leaves
     * either the complete old file or the complete new one.
     */
    public void rewrite(DataStore store) throws IOException {
        Path temp = path.resolveSibling(path.getFileName() + ".rewrite");
        try (FileChannel out = FileChannel.open(temp, CREATE, WRITE, TRUNCATE_EXISTING)) {
            OutputStream stream = new BufferedOutputStream(Channels.newOutputStream(out), 64 * 1024);
            for (String key : store.keys("*")) {
                DataStore.Entry entry = store.get(key);
                if (entry == null) {
                    continue;
                }
                writeValue(stream, key, entry.value());
                if (entry.expireAt() != 0) {
                    stream.write(Resp.array(List.of(PEXPIREAT, bytes(key), bytes(Long.toString(entry.expireAt())))));
                }
            }
            stream.flush();
            out.force(true);
        }
        // The snapshot already contains every buffered write, so the buffer can go.
        pending.reset();
        channel.close();
        Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        open();
        dirty = false;
    }

    private static void writeValue(OutputStream stream, String key, Object value) throws IOException {
        if (value instanceof RedisList list) {
            // Long lists go out as several RPUSH commands so no single command gets huge.
            List<byte[]> batch = new ArrayList<>(LIST_BATCH + 2);
            for (byte[] item : list) {
                if (batch.isEmpty()) {
                    batch.add(RPUSH);
                    batch.add(bytes(key));
                }
                batch.add(item);
                if (batch.size() == LIST_BATCH + 2) {
                    stream.write(Resp.array(batch));
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                stream.write(Resp.array(batch));
            }
        } else {
            stream.write(Resp.array(List.of(SET, bytes(key), (byte[]) value)));
        }
    }

    @Override
    public void close() throws IOException {
        if (channel != null) {
            sync();
            channel.close();
            channel = null;
        }
    }
}
