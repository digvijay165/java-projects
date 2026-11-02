package io.github.digvijay165.miniredis.store;

import java.util.ArrayDeque;

/** A Redis list: a double-ended queue, so pushing and popping at either end is O(1). */
public final class RedisList extends ArrayDeque<byte[]> {
    private static final long serialVersionUID = 1L;
}
