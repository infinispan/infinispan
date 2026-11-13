package org.infinispan.query.objectfilter.impl.ql.test;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.query.objectfilter.ParsingException;
import org.infinispan.query.objectfilter.impl.ql.QueryParser;
import org.testng.annotations.Test;

/**
 * Tests exposing gaps in the Ickle nesting depth guard ({@code infinispan.query.max-nesting-depth}).
 * <p>
 * Whatever the query, {@link QueryParser#parseQuery(String)} must either succeed or throw a {@link ParsingException}.
 * It must never leak a {@link StackOverflowError}, and it must not reject valid queries.
 * <p>
 * The depths below are above the default limit (100) so they trigger the guard when it works. The NOT and path index
 * tests use a depth that overflows the stack of the ANTLR recursive-descent parser.
 */
@Test(testName = "query.objectfilter.impl.ql.test.QueryParserNestingDepthTest", groups = "functional")
public class QueryParserNestingDepthTest {

   private static final String FROM = "FROM Note WHERE ";

   // Well above the default limit of 100, but small enough to parse quickly when the guard is (wrongly) bypassed
   private static final int PAREN_DEPTH = 300;

   // Deep enough to overflow the stack of the parser thread
   private static final int OVERFLOW_DEPTH = 10_000;

   /**
    * Control: plain parentheses beyond the limit are rejected with a ParsingException.
    */
   public void testDeepParenthesesRejected() {
      assertRejectedWithNestingError(FROM + "(".repeat(PAREN_DEPTH) + "name = 'foo'" + ")".repeat(PAREN_DEPTH));
   }

   /**
    * A backslash-escaped apostrophe is a valid escape sequence in the lexer, but the guard toggles its "in string"
    * state on every apostrophe, so the parentheses that follow are considered to be inside a string and are not counted.
    */
   public void testEscapedApostropheDoesNotHideParentheses() {
      String query = FROM + "name = 'a\\'b' AND " + "(".repeat(PAREN_DEPTH) + "name = 'foo'" + ")".repeat(PAREN_DEPTH);
      assertRejectedWithNestingError(query);
   }

   /**
    * An apostrophe inside a double-quoted string literal is not a string delimiter, but the guard toggles on it,
    * so the parentheses that follow are not counted.
    */
   public void testApostropheInDoubleQuotedStringDoesNotHideParentheses() {
      String query = FROM + "name = \"it's\" AND " + "(".repeat(PAREN_DEPTH) + "name = 'foo'" + ")".repeat(PAREN_DEPTH);
      assertRejectedWithNestingError(query);
   }

   /**
    * Parentheses inside a double-quoted string literal are data, not nesting. This is a valid query.
    */
   public void testParenthesesInDoubleQuotedStringNotCounted() {
      assertParses(FROM + "name = \"" + "(".repeat(150) + "\"");
   }

   /**
    * Parentheses inside a regexp literal (only valid in a full-text predicate) are data, not nesting. This is a valid query.
    */
   public void testParenthesesInRegexpLiteralNotCounted() {
      assertParses(FROM + "name : /" + "\\(".repeat(150) + "/");
   }

   /**
    * Each NOT recurses once in the parser ({@code negatedExpression : not_key negatedExpression}) but uses no parentheses.
    */
   public void testLongNotChain() {
      assertNoStackOverflow(FROM + "NOT ".repeat(OVERFLOW_DEPTH * 10) + "name = 'foo'");
   }

   /**
    * A path index may contain an expression, which may contain a path, and so on ({@code path : ... LSQUARE expression RSQUARE}).
    * Square brackets are not counted by the guard.
    */
   public void testDeepPathIndexNesting() {
      String query = "FROM Note n WHERE n.a" + "[n.a".repeat(OVERFLOW_DEPTH) + "]".repeat(OVERFLOW_DEPTH) + " = 1";
      assertNoStackOverflow(query);
   }

   /**
    * @return {@code null} if the query is accepted, otherwise a short description of the failure. The Throwable itself
    * is deliberately not passed to AssertJ: rendering a StackOverflowError with its huge stack trace is very slow.
    */
   private static String parse(String query) {
      try {
         new QueryParser().parseQuery(query);
         return null;
      } catch (ParsingException e) {
         return "ParsingException: " + e.getMessage();
      } catch (Throwable t) {
         return t.getClass().getName();
      }
   }

   private static void assertParses(String query) {
      assertThat(parse(query)).as("query should be accepted").isNull();
   }

   private static void assertNoStackOverflow(String query) {
      String failure = parse(query);
      assertThat(failure == null || !failure.contains("StackOverflowError"))
            .as("must succeed or fail with a ParsingException, but got: %s", failure).isTrue();
   }

   private static void assertRejectedWithNestingError(String query) {
      assertThat(parse(query)).as("expected the nesting guard to reject the query").contains("ISPN028535");
   }
}
