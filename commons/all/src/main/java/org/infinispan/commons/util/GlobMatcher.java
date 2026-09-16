package org.infinispan.commons.util;

import java.nio.charset.StandardCharsets;

/**
 * A matcher for glob patterns.
 *
 * <p>
 * Matches the glob pattern without transforming into a {@link java.util.regex.Pattern}. Otherwise, it is possible to
 * convert with {@link GlobUtils#globToRegex(String)}.
 * </p>
 *
 * @since 15.0
 * @author José Bolina
 * @see <a href="https://github.com/valkey-io/valkey/blob/dd4bd5065b6c36ff2dfcf41a49781e5f659f4d41/src/util.c#L57">Valkey implementation.</a>
 */
public final class GlobMatcher {

   private GlobMatcher() { }

   /**
    * Checks whether the given string matches the glob pattern.
    *
    * <p>
    * <b>Warning:</b> This method does not accept multibyte strings.
    * </p>
    *
    * @param pattern: Glob pattern to check against.
    * @param string: String to check for matching.
    * @return <code>true</code> if a match is found, <code>false</code>, otherwise.
    * @throws AssertionError If the {@param string} is a multibyte string.
    */
   public static boolean match(String pattern, String string) {
      assertSingleByte(string);
      return match(pattern.getBytes(StandardCharsets.US_ASCII), string.getBytes(StandardCharsets.US_ASCII));
   }

   /**
    * Checks whether the given byte sequence matches the glob pattern describe as a byte sequence.
    *
    * <p>
    * <b>Warning</b>: This method does not accept multibyte characters.
    * </p>
    *
    * @param pattern: The glob pattern represented as a byte sequence.
    * @param string: The string represented as a byte sequence.
    * @return <code>true</code> if a match is found, <code>false</code>, otherwise.
    */
   public static boolean match(byte[] pattern, byte[] string) {
      assertSingleByte(string);
      ByRef.Boolean skipLongerMatches = new ByRef.Boolean(false);
      return match(pattern, 0, string, 0, skipLongerMatches);
   }

   private static boolean match(byte[] pattern, int patternPos, byte[] string, int stringPos, ByRef.Boolean skipLongerMatches) {
      while (patternPos < pattern.length && stringPos < string.length) {
         switch (pattern[patternPos]) {

            // A star matches zero or more characters.
            case '*':
               // Consume sequence of star characters.
               // A `**` allows for recursive searches.
               while (patternPos < pattern.length - 1 && pattern[patternPos + 1] == '*') {
                  patternPos++;
               }

               // If last character is star, we don't need to check a match.
               if (patternPos == pattern.length) return true;

               // Try to recursively match the * with the characters in the string.
               // Utilize the ByRef.Boolean to avoid an explosion in the backtracking.
               while (stringPos <= string.length) {
                  if (match(pattern, patternPos + 1, string, stringPos, skipLongerMatches))
                     return true;

                  if (skipLongerMatches.get()) return false;

                  stringPos++;
               }

               skipLongerMatches.set(true);
               return false;

            // Matches exactly one character.
            case '?':
               stringPos++;
               break;

            // Start a range of characters to match.
            // The `^` character to negate the sequence in the range.
            case '[':
               patternPos++;

               // Reached the end of the pattern *before* ending the range.
               // We do a character comparison.
               if (patternPos == pattern.length) {
                  patternPos--;
                  if (pattern[patternPos] == string[stringPos]) stringPos++;
                  else return false;
                  break;
               }

               // Check for the negation character at the start of the range.
               boolean notOp = pattern[patternPos] == '^';
               if (notOp) {
                  patternPos++;
               }

               // Check for matches or the end of range.
               boolean match = false;
               while (true) {
                  // Consume the complete pattern without closing the range.
                  if (patternPos == pattern.length) {
                     patternPos--;
                     break;
                  }

                  // Escape character in the string.
                  if (patternPos < pattern.length - 1 && pattern[patternPos] == '\\') {
                     patternPos++;

                     if (pattern[patternPos] == string[stringPos])
                        match = true;

                  // End of the range.
                  } else if (pattern[patternPos] == ']') {
                     break;

                  // Identifies a complete range between two characters. The dash represent the start and end.
                  } else if (patternPos < pattern.length - 2 && pattern[patternPos + 1] == '-') {
                     byte start = pattern[patternPos];
                     byte end = pattern[patternPos + 2];
                     byte c = string[stringPos];

                     if (start > end) {
                        byte t = start;
                        start = end;
                        end = t;
                     }

                     patternPos += 2;

                     if (c >= start && c <= end) match = true;

                  // Else, we check for a direct match between the characters in the range and the string.
                  // While within the range, just a single match is enough to consider the range valid.
                  } else {
                     match |= pattern[patternPos] == string[stringPos];
                  }

                  patternPos++;
               }

               // If there was a negation in the range, we check for the inverse.
               if (notOp) match = !match;
               if (!match) return false;

               stringPos++;
               break;

            // Escaping a character in the sequence. We consume the escape and fallthrough to check for a match.
            case '\\':
               if (patternPos < pattern.length - 1) patternPos++;

               // Fallthrough.

            // Verify if the characters match.
            default:
               if (pattern[patternPos] != string[stringPos])
                  return false;

               stringPos++;
               break;
         }

         // If we consumed the string, but there is still missing characters on the pattern.
         // The remaining characters need to be `*`.
         patternPos++;
         if (stringPos == string.length) {
            while (patternPos < pattern.length && pattern[patternPos] == '*') {
               patternPos++;
            }
            break;
         }
      }

      // Only a match if we consumed both the pattern and the sequence.
      return patternPos == pattern.length && stringPos == string.length;
   }

   private static void assertSingleByte(String s) {
      byte[] bytes = s.getBytes();
      assertSingleByte(bytes);
   }

   private static void assertSingleByte(byte[] bytes) {
      for (byte b : bytes) {
         assert (b & 0x80) == 0 : "Multi-byte character not accepted";
      }
   }
}
