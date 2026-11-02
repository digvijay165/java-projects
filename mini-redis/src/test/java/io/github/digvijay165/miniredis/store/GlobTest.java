package io.github.digvijay165.miniredis.store;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GlobTest {
    @ParameterizedTest(name = "\"{0}\" vs \"{1}\" -> {2}")
    @CsvSource(delimiter = '|', value = {
            "*         | anything       | true",
            "*         | ''             | true",
            "h?llo     | hello          | true",
            "h?llo     | hllo           | false",
            "h*llo     | hllo           | true",
            "h*llo     | heeeello       | true",
            "h[ae]llo  | hallo          | true",
            "h[ae]llo  | hillo          | false",
            "h[^e]llo  | hallo          | true",
            "h[^e]llo  | hello          | false",
            "h[a-c]llo | hbllo          | true",
            "h[a-c]llo | hdllo          | false",
            "user:*:id | user:42:id     | true",
            "user:*:id | user:42:name   | false",
            "a\\*b     | a*b            | true",
            "a\\*b     | axb            | false",
            "[abc      | [abc           | true",
            "*a*b*c    | xaybzc         | true",
            "*a*b*c    | acb            | false",
            "a*        | ''             | false",
    })
    void matchesLikeRedis(String pattern, String text, boolean expected) {
        assertEquals(expected, Glob.matches(pattern, text));
    }
}
