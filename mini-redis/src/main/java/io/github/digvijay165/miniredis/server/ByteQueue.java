package io.github.digvijay165.miniredis.server;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A growable byte buffer that is read from the front and written at the back. Each connection has
 * one for the requests it has received and one for the replies still to be sent.
 */
final class ByteQueue {
    private static final int MAX_CAPACITY = Integer.MAX_VALUE - 8;
    private static final int SHRINK_ABOVE = 1024 * 1024;

    private final int initialCapacity;
    private byte[] data;
    private int start;
    private int end;

    ByteQueue(int initialCapacity) {
        this.initialCapacity = initialCapacity;
        this.data = new byte[initialCapacity];
    }

    byte[] array() {
        return data;
    }

    int start() {
        return start;
    }

    int end() {
        return end;
    }

    int readable() {
        return end - start;
    }

    /** Makes room for at least n more bytes at the back, compacting first and growing only if that is not enough. */
    void ensureWritable(int n) {
        if (data.length - end >= n) {
            return;
        }
        int readable = readable();
        if (start > 0) {
            System.arraycopy(data, start, data, 0, readable);
            start = 0;
            end = readable;
            if (data.length - end >= n) {
                return;
            }
        }
        long needed = (long) readable + n;
        if (needed > MAX_CAPACITY) {
            throw new IllegalStateException("buffer would exceed " + MAX_CAPACITY + " bytes");
        }
        int capacity = data.length;
        while (capacity < needed) {
            capacity = (int) Math.min((long) capacity * 2, MAX_CAPACITY);
        }
        data = Arrays.copyOf(data, capacity);
    }

    void append(byte[] bytes) {
        ensureWritable(bytes.length);
        System.arraycopy(bytes, 0, data, end, bytes.length);
        end += bytes.length;
    }

    /** The free space at the back, for a socket read. Call {@link #written} with the byte count afterwards. */
    ByteBuffer writableBuffer() {
        return ByteBuffer.wrap(data, end, data.length - end);
    }

    void written(int n) {
        end += n;
    }

    /** The bytes waiting at the front, for a socket write. Call {@link #consume} with the byte count afterwards. */
    ByteBuffer readableBuffer() {
        return ByteBuffer.wrap(data, start, readable());
    }

    void consume(int n) {
        start += n;
        if (start == end) {
            start = 0;
            end = 0;
            // Give back the memory after a one-off large value has been dealt with.
            if (data.length > SHRINK_ABOVE) {
                data = new byte[initialCapacity];
            }
        }
    }
}
