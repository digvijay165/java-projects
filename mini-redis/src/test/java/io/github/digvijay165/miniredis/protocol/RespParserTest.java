package io.github.digvijay165.miniredis.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static io.github.digvijay165.miniredis.protocol.Bytes.bytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RespParserTest {
    private static RespParser.Command parse(String input) throws ProtocolException {
        byte[] buf = bytes(input);
        return RespParser.parse(buf, 0, buf.length);
    }

    private static List<String> strings(RespParser.Command command) {
        return command.args().stream().map(Bytes::str).toList();
    }

    @Test
    void parsesAnArrayOfBulkStrings() throws ProtocolException {
        String input = "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        RespParser.Command command = parse(input);
        assertEquals(List.of("GET", "foo"), strings(command));
        assertEquals(input.length(), command.length());
    }

    @Test
    void waitsForMoreBytesUntilTheCommandIsComplete() throws ProtocolException {
        byte[] buf = bytes("*2\r\n$3\r\nSET\r\n$5\r\nhello\r\n");
        for (int end = 0; end < buf.length; end++) {
            assertNull(RespParser.parse(buf, 0, end), "prefix of " + end + " bytes");
        }
        assertNotNull(RespParser.parse(buf, 0, buf.length));
    }

    @Test
    void readsPipelinedCommandsOneAtATime() throws ProtocolException {
        byte[] buf = bytes("*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n");
        RespParser.Command first = RespParser.parse(buf, 0, buf.length);
        RespParser.Command second = RespParser.parse(buf, first.length(), buf.length);
        assertEquals(List.of("PING"), strings(first));
        assertEquals(List.of("ECHO", "hi"), strings(second));
        assertEquals(buf.length, first.length() + second.length());
    }

    @Test
    void bulkStringsAreBinarySafe() throws ProtocolException {
        byte[] value = {'a', '\r', '\n', 0, (byte) 0xff};
        byte[] buf = Resp.array(List.of(bytes("SET"), bytes("k"), value));
        RespParser.Command command = RespParser.parse(buf, 0, buf.length);
        assertArrayEquals(value, command.args().get(2));
    }

    @Test
    void parsesInlineCommandsLikeTelnetSends() throws ProtocolException {
        assertEquals(List.of("PING"), strings(parse("PING\r\n")));
        assertEquals(List.of("SET", "a", "b"), strings(parse("SET  a\tb\n")));
        assertNull(parse("PING"));
    }

    @Test
    void emptyArrayGivesAnEmptyCommand() throws ProtocolException {
        RespParser.Command command = parse("*0\r\n");
        assertEquals(List.of(), command.args());
        assertEquals(4, command.length());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "*1\r\n+OK\r\n",                  // element is not a bulk string
            "*x\r\n",                         // count is not a number
            "*1\r\n$-5\r\n",                  // negative bulk length
            "*1\r\n$3\r\nabcXY",              // bulk string not followed by CRLF
            "*1\r\n$999999999999\r\n",        // bulk string bigger than 512 MB
            "*99999999\r\n",                  // too many elements
            "*1\r\n$1234567890123456789012345678901234567890\r\n", // header line too long
    })
    void rejectsMalformedInput(String input) {
        assertThrows(ProtocolException.class, () -> parse(input));
    }
}
