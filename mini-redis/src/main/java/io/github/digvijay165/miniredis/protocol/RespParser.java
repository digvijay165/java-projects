package io.github.digvijay165.miniredis.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Parses client requests.
 *
 * <p>Clients normally send a RESP array of bulk strings, for example
 * {@code *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n}. "Inline" commands typed into telnet, like
 * {@code GET key\r\n}, work too.
 *
 * <p>The parser is stateless: pass it the bytes received so far and it returns the first complete
 * command, or null if more bytes are needed. Several pipelined commands in one buffer are read one
 * call at a time.
 */
public final class RespParser {
    public static final int MAX_BULK_LENGTH = 512 * 1024 * 1024;
    public static final int MAX_ARRAY_LENGTH = 1024 * 1024;
    public static final int MAX_INLINE_LENGTH = 64 * 1024;
    private static final int MAX_HEADER_LENGTH = 32;

    /** A parsed command and how many bytes of input it used. */
    public record Command(List<byte[]> args, int length) {
    }

    private RespParser() {
    }

    public static Command parse(byte[] buf, int start, int end) throws ProtocolException {
        if (start >= end) {
            return null;
        }
        return buf[start] == '*' ? parseArray(buf, start, end) : parseInline(buf, start, end);
    }

    private static Command parseArray(byte[] buf, int start, int end) throws ProtocolException {
        int lineEnd = headerEnd(buf, start + 1, end);
        if (lineEnd < 0) {
            return null;
        }
        long count = parseLength(buf, start + 1, lineEnd, "invalid multibulk length");
        if (count > MAX_ARRAY_LENGTH) {
            throw new ProtocolException("invalid multibulk length");
        }
        int position = lineEnd + 2;
        if (count <= 0) {
            return new Command(List.of(), position - start);
        }

        List<byte[]> args = new ArrayList<>((int) Math.min(count, 64));
        for (long i = 0; i < count; i++) {
            if (position >= end) {
                return null;
            }
            if (buf[position] != '$') {
                throw new ProtocolException("expected '$', got '" + printable(buf[position]) + "'");
            }
            int bulkHeaderEnd = headerEnd(buf, position + 1, end);
            if (bulkHeaderEnd < 0) {
                return null;
            }
            long length = parseLength(buf, position + 1, bulkHeaderEnd, "invalid bulk length");
            if (length < 0 || length > MAX_BULK_LENGTH) {
                throw new ProtocolException("invalid bulk length");
            }
            long dataStart = bulkHeaderEnd + 2L;
            long dataEnd = dataStart + length;
            if (dataEnd + 2 > end) {
                return null;
            }
            if (buf[(int) dataEnd] != '\r' || buf[(int) dataEnd + 1] != '\n') {
                throw new ProtocolException("bulk string is not followed by CRLF");
            }
            args.add(Arrays.copyOfRange(buf, (int) dataStart, (int) dataEnd));
            position = (int) dataEnd + 2;
        }
        return new Command(args, position - start);
    }

    private static Command parseInline(byte[] buf, int start, int end) throws ProtocolException {
        int newline = -1;
        int limit = Math.min(end, start + MAX_INLINE_LENGTH + 2);
        for (int i = start; i < limit; i++) {
            if (buf[i] == '\n') {
                newline = i;
                break;
            }
        }
        if (newline < 0) {
            if (end - start > MAX_INLINE_LENGTH) {
                throw new ProtocolException("too big inline request");
            }
            return null;
        }

        int lineEnd = newline > start && buf[newline - 1] == '\r' ? newline - 1 : newline;
        List<byte[]> args = new ArrayList<>();
        int i = start;
        while (i < lineEnd) {
            while (i < lineEnd && isSpace(buf[i])) {
                i++;
            }
            int tokenStart = i;
            while (i < lineEnd && !isSpace(buf[i])) {
                i++;
            }
            if (i > tokenStart) {
                args.add(Arrays.copyOfRange(buf, tokenStart, i));
            }
        }
        return new Command(args, newline + 1 - start);
    }

    /** Finds the CRLF ending a header line that starts at from. Returns -1 if more bytes are needed. */
    private static int headerEnd(byte[] buf, int from, int end) throws ProtocolException {
        int limit = Math.min(end - 1, from + MAX_HEADER_LENGTH);
        for (int i = from; i < limit; i++) {
            if (buf[i] == '\r' && buf[i + 1] == '\n') {
                return i;
            }
        }
        if (end - from > MAX_HEADER_LENGTH) {
            throw new ProtocolException("header line too long");
        }
        return -1;
    }

    private static long parseLength(byte[] buf, int from, int to, String error) throws ProtocolException {
        boolean negative = from < to && buf[from] == '-';
        int i = negative ? from + 1 : from;
        if (i == to || to - i > 18) {
            throw new ProtocolException(error);
        }
        long value = 0;
        for (; i < to; i++) {
            int digit = buf[i] - '0';
            if (digit < 0 || digit > 9) {
                throw new ProtocolException(error);
            }
            value = value * 10 + digit;
        }
        return negative ? -value : value;
    }

    private static boolean isSpace(byte b) {
        return b == ' ' || b == '\t';
    }

    private static String printable(byte b) {
        return b >= 32 && b < 127 ? String.valueOf((char) b) : String.format("\\x%02x", b & 0xff);
    }
}
