package io.github.digvijay165.miniredis;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/** Minimal timestamped logging to the console, in the style of Redis's own log lines. */
public final class Log {
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private Log() {
    }

    public static void info(String message) {
        System.out.println(LocalTime.now().format(TIME) + " * " + message);
    }

    public static void warn(String message) {
        System.out.println(LocalTime.now().format(TIME) + " # " + message);
    }

    public static void error(String message, Throwable cause) {
        System.err.println(LocalTime.now().format(TIME) + " # " + message + ": " + cause);
    }
}
