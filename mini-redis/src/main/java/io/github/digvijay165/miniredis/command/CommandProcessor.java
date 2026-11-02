package io.github.digvijay165.miniredis.command;

import io.github.digvijay165.miniredis.protocol.Bytes;
import io.github.digvijay165.miniredis.protocol.Resp;
import io.github.digvijay165.miniredis.store.DataStore;
import io.github.digvijay165.miniredis.store.RedisList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

import static io.github.digvijay165.miniredis.protocol.Bytes.bytes;
import static io.github.digvijay165.miniredis.protocol.Bytes.str;

/**
 * Runs commands against the data store and encodes the replies.
 *
 * <p>Every command that changes data is also handed to a propagation sink, which the append-only
 * file uses to log it. Relative expiry times (SET ... EX, EXPIRE) are logged as absolute ones
 * (PEXPIREAT), so replaying the log after a restart gives keys their original deadline rather
 * than a fresh one.
 */
public final class CommandProcessor {
    /** An action that can fail with an IOException, such as rewriting the append-only file. */
    @FunctionalInterface
    public interface IoAction {
        void run() throws IOException;
    }

    @FunctionalInterface
    private interface Handler {
        byte[] handle(List<byte[]> args) throws CommandException;
    }

    /** arity counts the command name too: a positive number is exact, a negative one is a minimum. */
    private record Spec(Handler handler, int arity) {
    }

    private static final String VERSION = "1.0.0";
    private static final String WRONGTYPE = "WRONGTYPE Operation against a key holding the wrong kind of value";
    private static final String NOT_AN_INTEGER = "ERR value is not an integer or out of range";
    private static final String SYNTAX_ERROR = "ERR syntax error";

    private final DataStore store;
    private final Map<String, Spec> commands = new HashMap<>();
    private final long startedAt = System.currentTimeMillis();
    private Consumer<List<byte[]>> sink = command -> { };
    private IoAction aofRewriter;
    private long processed;

    public CommandProcessor(DataStore store) {
        this.store = store;

        // Connection and server
        register("ping", -1, this::ping);
        register("echo", 2, a -> Resp.bulk(a.get(1)));
        register("select", 2, this::select);
        register("dbsize", 1, a -> Resp.integer(store.size()));
        register("flushall", -1, this::flushAll);
        register("flushdb", -1, this::flushAll);
        register("info", -1, this::info);
        register("command", -1, a -> Resp.EMPTY_ARRAY);
        register("config", -2, this::config);
        register("client", -2, this::client);
        register("bgrewriteaof", 1, this::rewriteAof);

        // Keys
        register("del", -2, this::del);
        register("exists", -2, this::exists);
        register("type", 2, this::type);
        register("keys", 2, this::keys);
        register("expire", 3, a -> expire(a, 1000, false, "expire"));
        register("pexpire", 3, a -> expire(a, 1, false, "pexpire"));
        register("expireat", 3, a -> expire(a, 1000, true, "expireat"));
        register("pexpireat", 3, a -> expire(a, 1, true, "pexpireat"));
        register("ttl", 2, a -> ttl(a, false));
        register("pttl", 2, a -> ttl(a, true));
        register("persist", 2, this::persist);

        // Strings
        register("set", -3, this::set);
        register("get", 2, a -> Resp.bulk(getString(str(a.get(1)))));
        register("mset", -3, this::mset);
        register("mget", -2, this::mget);
        register("append", 3, this::append);
        register("strlen", 2, this::strlen);
        register("incr", 2, a -> incrBy(str(a.get(1)), 1));
        register("decr", 2, a -> incrBy(str(a.get(1)), -1));
        register("incrby", 3, a -> incrBy(str(a.get(1)), parseLong(a.get(2))));
        register("decrby", 3, a -> incrBy(str(a.get(1)), negate(parseLong(a.get(2)))));

        // Lists
        register("lpush", -3, a -> push(a, true));
        register("rpush", -3, a -> push(a, false));
        register("lpop", -2, a -> pop(a, true));
        register("rpop", -2, a -> pop(a, false));
        register("llen", 2, this::llen);
        register("lrange", 4, this::lrange);
    }

    private void register(String name, int arity, Handler handler) {
        commands.put(name, new Spec(handler, arity));
    }

    /** Where write commands go after they run, normally the append-only file. */
    public void setPropagation(Consumer<List<byte[]>> sink) {
        this.sink = sink;
    }

    /** What BGREWRITEAOF runs. Without one, the command replies that the append-only file is off. */
    public void setAofRewriter(IoAction rewriter) {
        this.aofRewriter = rewriter;
    }

    /** Runs one command (name first, then arguments) and returns the encoded reply. */
    public byte[] execute(List<byte[]> args) {
        processed++;
        String name = str(args.get(0)).toLowerCase(Locale.ROOT);
        Spec spec = commands.get(name);
        if (spec == null) {
            String shown = name.length() > 64 ? name.substring(0, 64) + "..." : name;
            return Resp.error("ERR unknown command '" + shown + "'");
        }
        int count = args.size();
        if (spec.arity() > 0 ? count != spec.arity() : count < -spec.arity()) {
            return Resp.error(wrongArgs(name));
        }
        try {
            return spec.handler().handle(args);
        } catch (CommandException e) {
            return Resp.error(e.getMessage());
        }
    }

    // ---- Connection and server -------------------------------------------------------------

    private byte[] ping(List<byte[]> a) throws CommandException {
        if (a.size() > 2) {
            throw new CommandException(wrongArgs("ping"));
        }
        return a.size() == 1 ? Resp.PONG : Resp.bulk(a.get(1));
    }

    private byte[] select(List<byte[]> a) throws CommandException {
        // There is a single database, 0. Client libraries often select it on connect.
        if (!str(a.get(1)).equals("0")) {
            throw new CommandException("ERR DB index is out of range");
        }
        return Resp.OK;
    }

    private byte[] flushAll(List<byte[]> a) {
        store.clear();
        propagate(command("FLUSHALL"));
        return Resp.OK;
    }

    private byte[] info(List<byte[]> a) {
        String text = "# Server\r\n"
                + "mini_redis_version:" + VERSION + "\r\n"
                + "uptime_in_seconds:" + (System.currentTimeMillis() - startedAt) / 1000 + "\r\n"
                + "\r\n# Stats\r\n"
                + "total_commands_processed:" + processed + "\r\n"
                + "\r\n# Keyspace\r\n"
                + "db0:keys=" + store.size() + ",expires=" + store.expiringCount() + "\r\n";
        return Resp.bulk(bytes(text));
    }

    private byte[] config(List<byte[]> a) throws CommandException {
        // redis-benchmark asks CONFIG GET on start-up. There are no settings to report.
        if (str(a.get(1)).equalsIgnoreCase("get")) {
            return Resp.EMPTY_ARRAY;
        }
        throw new CommandException("ERR unsupported CONFIG subcommand");
    }

    private byte[] client(List<byte[]> a) throws CommandException {
        // Client libraries send these when they connect.
        return switch (str(a.get(1)).toLowerCase(Locale.ROOT)) {
            case "setname", "setinfo" -> Resp.OK;
            case "getname" -> Resp.NULL_BULK;
            default -> throw new CommandException("ERR unsupported CLIENT subcommand");
        };
    }

    private byte[] rewriteAof(List<byte[]> a) throws CommandException {
        if (aofRewriter == null) {
            throw new CommandException("ERR append only file is turned off");
        }
        try {
            aofRewriter.run();
        } catch (IOException e) {
            throw new CommandException("ERR rewriting the append only file failed: " + e.getMessage());
        }
        return Resp.simple("Append only file rewritten");
    }

    // ---- Keys ------------------------------------------------------------------------------

    private byte[] del(List<byte[]> a) {
        int removed = 0;
        for (int i = 1; i < a.size(); i++) {
            String key = str(a.get(i));
            if (store.get(key) != null && store.remove(key)) {
                removed++;
            }
        }
        if (removed > 0) {
            propagate(a);
        }
        return Resp.integer(removed);
    }

    private byte[] exists(List<byte[]> a) {
        int found = 0;
        for (int i = 1; i < a.size(); i++) {
            if (store.get(str(a.get(i))) != null) {
                found++;
            }
        }
        return Resp.integer(found);
    }

    private byte[] type(List<byte[]> a) {
        Object value = value(str(a.get(1)));
        String type = value == null ? "none" : value instanceof RedisList ? "list" : "string";
        return Resp.simple(type);
    }

    private byte[] keys(List<byte[]> a) {
        return Resp.array(store.keys(str(a.get(1))).stream().map(Bytes::bytes).toList());
    }

    private byte[] expire(List<byte[]> a, long unitMillis, boolean absolute, String name) throws CommandException {
        String key = str(a.get(1));
        long amount = parseLong(a.get(2));
        long at;
        try {
            long millis = Math.multiplyExact(amount, unitMillis);
            at = absolute ? millis : Math.addExact(store.now(), millis);
        } catch (ArithmeticException e) {
            throw invalidExpire(name);
        }
        if (!store.expireAt(key, at)) {
            return Resp.integer(0);
        }
        // A deadline in the past deletes the key, so log a DEL in that case.
        propagate(store.get(key) == null ? command("DEL", key) : command("PEXPIREAT", key, at));
        return Resp.integer(1);
    }

    private byte[] ttl(List<byte[]> a, boolean millis) {
        DataStore.Entry entry = store.get(str(a.get(1)));
        if (entry == null) {
            return Resp.integer(-2);
        }
        if (entry.expireAt() == 0) {
            return Resp.integer(-1);
        }
        long left = entry.expireAt() - store.now();
        return Resp.integer(millis ? left : (left + 500) / 1000);
    }

    private byte[] persist(List<byte[]> a) {
        if (!store.persist(str(a.get(1)))) {
            return Resp.integer(0);
        }
        propagate(a);
        return Resp.integer(1);
    }

    // ---- Strings ---------------------------------------------------------------------------

    private byte[] set(List<byte[]> a) throws CommandException {
        String key = str(a.get(1));
        byte[] value = a.get(2);
        boolean nx = false;
        boolean xx = false;
        boolean keepTtl = false;
        boolean get = false;
        long expireAt = 0;
        for (int i = 3; i < a.size(); i++) {
            String option = str(a.get(i)).toUpperCase(Locale.ROOT);
            switch (option) {
                case "NX" -> nx = true;
                case "XX" -> xx = true;
                case "KEEPTTL" -> keepTtl = true;
                case "GET" -> get = true;
                case "EX", "PX" -> {
                    if (expireAt != 0 || i + 1 == a.size()) {
                        throw new CommandException(SYNTAX_ERROR);
                    }
                    long amount = parseLong(a.get(++i));
                    if (amount <= 0) {
                        throw invalidExpire("set");
                    }
                    try {
                        long unit = option.equals("EX") ? 1000 : 1;
                        expireAt = Math.addExact(store.now(), Math.multiplyExact(amount, unit));
                    } catch (ArithmeticException e) {
                        throw invalidExpire("set");
                    }
                }
                default -> throw new CommandException(SYNTAX_ERROR);
            }
        }
        if ((nx && xx) || (keepTtl && expireAt != 0)) {
            throw new CommandException(SYNTAX_ERROR);
        }

        Object old = value(key);
        if (get && old != null && !(old instanceof byte[])) {
            throw new CommandException(WRONGTYPE);
        }
        boolean apply = nx ? old == null : !xx || old != null;
        if (apply) {
            store.put(key, value, keepTtl);
            if (expireAt != 0) {
                store.expireAt(key, expireAt);
            }
            propagate(keepTtl ? command("SET", key, value, "KEEPTTL") : command("SET", key, value));
            if (expireAt != 0) {
                propagate(command("PEXPIREAT", key, expireAt));
            }
        }
        if (get) {
            return Resp.bulk((byte[]) old);
        }
        return apply ? Resp.OK : Resp.NULL_BULK;
    }

    private byte[] mset(List<byte[]> a) throws CommandException {
        if (a.size() % 2 == 0) {
            throw new CommandException(wrongArgs("mset"));
        }
        for (int i = 1; i < a.size(); i += 2) {
            store.put(str(a.get(i)), a.get(i + 1), false);
        }
        propagate(a);
        return Resp.OK;
    }

    private byte[] mget(List<byte[]> a) {
        List<byte[]> values = new ArrayList<>(a.size() - 1);
        for (int i = 1; i < a.size(); i++) {
            // MGET never fails: a missing key or a key of another type gives null.
            values.add(value(str(a.get(i))) instanceof byte[] b ? b : null);
        }
        return Resp.array(values);
    }

    private byte[] append(List<byte[]> a) throws CommandException {
        String key = str(a.get(1));
        byte[] current = getString(key);
        byte[] suffix = a.get(2);
        byte[] result = suffix;
        if (current != null) {
            result = new byte[current.length + suffix.length];
            System.arraycopy(current, 0, result, 0, current.length);
            System.arraycopy(suffix, 0, result, current.length, suffix.length);
        }
        store.put(key, result, true);
        propagate(a);
        return Resp.integer(result.length);
    }

    private byte[] strlen(List<byte[]> a) throws CommandException {
        byte[] value = getString(str(a.get(1)));
        return Resp.integer(value == null ? 0 : value.length);
    }

    private byte[] incrBy(String key, long delta) throws CommandException {
        byte[] current = getString(key);
        long value = 0;
        if (current != null) {
            Long parsed = parseStrictLong(current);
            if (parsed == null) {
                throw new CommandException(NOT_AN_INTEGER);
            }
            value = parsed;
        }
        long result;
        try {
            result = Math.addExact(value, delta);
        } catch (ArithmeticException e) {
            throw new CommandException("ERR increment or decrement would overflow");
        }
        String text = Long.toString(result);
        store.put(key, bytes(text), true);
        // Log the result rather than the increment: replaying a SET is exact.
        propagate(command("SET", key, text, "KEEPTTL"));
        return Resp.integer(result);
    }

    // ---- Lists -----------------------------------------------------------------------------

    private byte[] push(List<byte[]> a, boolean left) throws CommandException {
        String key = str(a.get(1));
        RedisList list = getList(key);
        if (list == null) {
            list = new RedisList();
            store.put(key, list, false);
        }
        for (int i = 2; i < a.size(); i++) {
            if (left) {
                list.addFirst(a.get(i));
            } else {
                list.addLast(a.get(i));
            }
        }
        propagate(a);
        return Resp.integer(list.size());
    }

    private byte[] pop(List<byte[]> a, boolean left) throws CommandException {
        if (a.size() > 3) {
            throw new CommandException(wrongArgs(left ? "lpop" : "rpop"));
        }
        boolean withCount = a.size() == 3;
        long count = 1;
        if (withCount) {
            count = parseLong(a.get(2));
            if (count < 0) {
                throw new CommandException("ERR value is out of range, must be positive");
            }
        }
        String key = str(a.get(1));
        RedisList list = getList(key);
        if (list == null) {
            return withCount ? Resp.NULL_ARRAY : Resp.NULL_BULK;
        }
        List<byte[]> popped = new ArrayList<>();
        while (popped.size() < count && !list.isEmpty()) {
            popped.add(left ? list.pollFirst() : list.pollLast());
        }
        if (list.isEmpty()) {
            store.remove(key); // Redis never keeps an empty list
        }
        if (!popped.isEmpty()) {
            propagate(a);
        }
        return withCount ? Resp.array(popped) : Resp.bulk(popped.get(0));
    }

    private byte[] llen(List<byte[]> a) throws CommandException {
        RedisList list = getList(str(a.get(1)));
        return Resp.integer(list == null ? 0 : list.size());
    }

    private byte[] lrange(List<byte[]> a) throws CommandException {
        long start = parseLong(a.get(2));
        long stop = parseLong(a.get(3));
        RedisList list = getList(str(a.get(1)));
        if (list == null) {
            return Resp.EMPTY_ARRAY;
        }
        // Negative indexes count from the end: -1 is the last element.
        int size = list.size();
        if (start < 0) {
            start = Math.max(0, size + start);
        }
        if (stop < 0) {
            stop = size + stop;
        }
        stop = Math.min(stop, size - 1);
        if (start > stop) {
            return Resp.EMPTY_ARRAY;
        }
        List<byte[]> range = new ArrayList<>((int) (stop - start + 1));
        int index = 0;
        for (byte[] item : list) {
            if (index > stop) {
                break;
            }
            if (index >= start) {
                range.add(item);
            }
            index++;
        }
        return Resp.array(range);
    }

    // ---- Helpers ---------------------------------------------------------------------------

    private Object value(String key) {
        DataStore.Entry entry = store.get(key);
        return entry == null ? null : entry.value();
    }

    private byte[] getString(String key) throws CommandException {
        Object value = value(key);
        if (value == null || value instanceof byte[]) {
            return (byte[]) value;
        }
        throw new CommandException(WRONGTYPE);
    }

    private RedisList getList(String key) throws CommandException {
        Object value = value(key);
        if (value == null || value instanceof RedisList) {
            return (RedisList) value;
        }
        throw new CommandException(WRONGTYPE);
    }

    private void propagate(List<byte[]> command) {
        sink.accept(command);
    }

    private static List<byte[]> command(Object... parts) {
        List<byte[]> command = new ArrayList<>(parts.length);
        for (Object part : parts) {
            command.add(part instanceof byte[] b ? b : bytes(String.valueOf(part)));
        }
        return command;
    }

    private static long parseLong(byte[] arg) throws CommandException {
        Long value = parseStrictLong(arg);
        if (value == null) {
            throw new CommandException(NOT_AN_INTEGER);
        }
        return value;
    }

    private static long negate(long value) throws CommandException {
        if (value == Long.MIN_VALUE) {
            throw new CommandException("ERR decrement would overflow");
        }
        return -value;
    }

    /**
     * Parses a 64-bit integer as strictly as Redis does: no spaces, no '+', no leading zeros.
     * Returns null if the bytes are not exactly such a number.
     */
    static Long parseStrictLong(byte[] b) {
        if (b.length == 0 || b.length > 20) {
            return null;
        }
        boolean negative = b[0] == '-';
        int i = negative ? 1 : 0;
        if (i == b.length || (b[i] == '0' && (b.length - i > 1 || negative))) {
            return null;
        }
        // Accumulate as a negative number so Long.MIN_VALUE fits.
        long value = 0;
        for (; i < b.length; i++) {
            int digit = b[i] - '0';
            if (digit < 0 || digit > 9 || value < (Long.MIN_VALUE + digit) / 10) {
                return null;
            }
            value = value * 10 - digit;
        }
        if (negative) {
            return value;
        }
        return value == Long.MIN_VALUE ? null : -value;
    }

    private static String wrongArgs(String name) {
        return "ERR wrong number of arguments for '" + name + "' command";
    }

    private static CommandException invalidExpire(String name) {
        return new CommandException("ERR invalid expire time in '" + name + "' command");
    }
}
