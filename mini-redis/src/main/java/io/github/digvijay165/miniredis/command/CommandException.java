package io.github.digvijay165.miniredis.command;

/** A command failed. The message becomes the error reply, so it starts with a code such as ERR. */
final class CommandException extends Exception {
    private static final long serialVersionUID = 1L;

    CommandException(String message) {
        // No stack trace: these are ordinary replies, not bugs.
        super(message, null, false, false);
    }
}
