package io.github.digvijay165.miniredis.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

/**
 * The keyspace. Values are byte[] (strings) or {@link RedisList}.
 *
 * <p>Keys with a TTL expire two ways, as in Redis. Lazily: any access to an expired key deletes it
 * first, so callers never see one. Actively: {@link #expireCycle} samples random keys that have a
 * TTL and deletes the expired ones, so keys nobody reads again still free their memory.
 *
 * <p>Not thread-safe: the server's single event loop is the only thread that touches it.
 */
public final class DataStore {
    /** A value and its expiry time in epoch milliseconds (0 means it never expires). */
    public static final class Entry {
        private Object value;
        private long expireAt;

        private Entry(Object value) {
            this.value = value;
        }

        public Object value() {
            return value;
        }

        public long expireAt() {
            return expireAt;
        }
    }

    private static final int EXPIRE_SAMPLE_SIZE = 20;

    private final Map<String, Entry> entries = new HashMap<>();
    // Keys that have a TTL, also kept in a list so the expiry cycle can pick them at random in O(1).
    private final List<String> volatileKeys = new ArrayList<>();
    private final Map<String, Integer> volatileIndex = new HashMap<>();
    private final LongSupplier clock;

    public DataStore() {
        this(System::currentTimeMillis);
    }

    /** Uses the given clock (epoch milliseconds), so tests can move time forward. */
    public DataStore(LongSupplier clock) {
        this.clock = clock;
    }

    public long now() {
        return clock.getAsLong();
    }

    /** The live entry for key, or null. An expired key is deleted here. */
    public Entry get(String key) {
        Entry entry = entries.get(key);
        if (entry != null && entry.expireAt != 0 && entry.expireAt <= now()) {
            remove(key);
            return null;
        }
        return entry;
    }

    /** Stores value under key. Its TTL is removed unless keepTtl is set (SET clears it; INCR and APPEND keep it). */
    public void put(String key, Object value, boolean keepTtl) {
        Entry entry = get(key);
        if (entry == null) {
            entries.put(key, new Entry(value));
            return;
        }
        entry.value = value;
        if (!keepTtl) {
            clearTtl(key, entry);
        }
    }

    public boolean remove(String key) {
        Entry entry = entries.remove(key);
        if (entry == null) {
            return false;
        }
        if (entry.expireAt != 0) {
            untrack(key);
        }
        return true;
    }

    /** Sets an absolute expiry time. A time in the past deletes the key at once. Returns false if the key does not exist. */
    public boolean expireAt(String key, long at) {
        Entry entry = get(key);
        if (entry == null) {
            return false;
        }
        if (at <= now()) {
            remove(key);
            return true;
        }
        if (entry.expireAt == 0) {
            track(key);
        }
        entry.expireAt = at;
        return true;
    }

    /** Removes the key's TTL. Returns false if the key does not exist or has no TTL. */
    public boolean persist(String key) {
        Entry entry = get(key);
        if (entry == null || entry.expireAt == 0) {
            return false;
        }
        clearTtl(key, entry);
        return true;
    }

    /** Number of keys, including expired ones not yet collected (Redis's DBSIZE counts them too). */
    public int size() {
        return entries.size();
    }

    public int expiringCount() {
        return volatileKeys.size();
    }

    /** Live keys matching a glob pattern, in no particular order. */
    public List<String> keys(String pattern) {
        long now = now();
        boolean all = pattern.equals("*");
        List<String> matches = new ArrayList<>();
        for (Map.Entry<String, Entry> e : entries.entrySet()) {
            long expireAt = e.getValue().expireAt;
            boolean live = expireAt == 0 || expireAt > now;
            if (live && (all || Glob.matches(pattern, e.getKey()))) {
                matches.add(e.getKey());
            }
        }
        return matches;
    }

    public void clear() {
        entries.clear();
        volatileKeys.clear();
        volatileIndex.clear();
    }

    /**
     * Active expiry, the same algorithm Redis uses: sample up to 20 random keys that have a TTL and
     * delete the expired ones. If more than a quarter of the sample had expired, many more probably
     * have too, so sample again, until that stops being true or the time budget runs out.
     *
     * @return how many keys were deleted
     */
    public int expireCycle(long budgetNanos) {
        long deadline = System.nanoTime() + budgetNanos;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int removed = 0;
        while (!volatileKeys.isEmpty()) {
            long now = now();
            int sampled = Math.min(EXPIRE_SAMPLE_SIZE, volatileKeys.size());
            int expired = 0;
            for (int i = 0; i < sampled && !volatileKeys.isEmpty(); i++) {
                String key = volatileKeys.get(random.nextInt(volatileKeys.size()));
                if (entries.get(key).expireAt <= now) {
                    remove(key);
                    expired++;
                }
            }
            removed += expired;
            if (expired * 4 <= sampled || System.nanoTime() - deadline >= 0) {
                break;
            }
        }
        return removed;
    }

    private void clearTtl(String key, Entry entry) {
        if (entry.expireAt != 0) {
            untrack(key);
            entry.expireAt = 0;
        }
    }

    private void track(String key) {
        volatileIndex.put(key, volatileKeys.size());
        volatileKeys.add(key);
    }

    // Swap-remove: move the last key into the freed slot so removal stays O(1).
    private void untrack(String key) {
        int index = volatileIndex.remove(key);
        String last = volatileKeys.remove(volatileKeys.size() - 1);
        if (index < volatileKeys.size()) {
            volatileKeys.set(index, last);
            volatileIndex.put(last, index);
        }
    }
}
