package io.github.digvijay165.miniredis.store;

/** Redis-style glob patterns for KEYS: {@code *}, {@code ?}, {@code [abc]}, {@code [^abc]}, {@code [a-z]} and {@code \} to escape. */
public final class Glob {
    private Glob() {
    }

    public static boolean matches(String pattern, String text) {
        int p = 0;
        int t = 0;
        int starP = -1;
        int starT = -1;
        while (t < text.length()) {
            if (p < pattern.length()) {
                if (pattern.charAt(p) == '*') {
                    starP = p++;
                    starT = t;
                    continue;
                }
                int next = matchOne(pattern, p, text.charAt(t));
                if (next >= 0) {
                    p = next;
                    t++;
                    continue;
                }
            }
            // Mismatch: let the last '*' swallow one more character and try again.
            if (starP < 0) {
                return false;
            }
            p = starP + 1;
            t = ++starT;
        }
        while (p < pattern.length() && pattern.charAt(p) == '*') {
            p++;
        }
        return p == pattern.length();
    }

    /** Matches the pattern token at p against ch. Returns the index after the token, or -1 on a mismatch. */
    private static int matchOne(String pattern, int p, char ch) {
        char c = pattern.charAt(p);
        if (c == '?') {
            return p + 1;
        }
        if (c == '\\' && p + 1 < pattern.length()) {
            return pattern.charAt(p + 1) == ch ? p + 2 : -1;
        }
        if (c == '[') {
            int close = classEnd(pattern, p);
            if (close > 0) {
                return matchClass(pattern, p + 1, close, ch) ? close + 1 : -1;
            }
        }
        return c == ch ? p + 1 : -1;
    }

    /** Index of the ']' closing the class opened at p, or -1 if it is never closed (then '[' is literal). */
    private static int classEnd(String pattern, int p) {
        int i = p + 1;
        if (i < pattern.length() && pattern.charAt(i) == '^') {
            i++;
        }
        for (; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '\\') {
                i++;
            } else if (c == ']') {
                return i;
            }
        }
        return -1;
    }

    private static boolean matchClass(String pattern, int from, int close, char ch) {
        boolean negate = pattern.charAt(from) == '^';
        int i = negate ? from + 1 : from;
        boolean matched = false;
        while (i < close) {
            char c = pattern.charAt(i);
            if (c == '\\' && i + 1 < close) {
                matched |= pattern.charAt(i + 1) == ch;
                i += 2;
            } else if (i + 2 < close && pattern.charAt(i + 1) == '-') {
                char low = (char) Math.min(c, pattern.charAt(i + 2));
                char high = (char) Math.max(c, pattern.charAt(i + 2));
                matched |= ch >= low && ch <= high;
                i += 3;
            } else {
                matched |= c == ch;
                i++;
            }
        }
        return matched != negate;
    }
}
