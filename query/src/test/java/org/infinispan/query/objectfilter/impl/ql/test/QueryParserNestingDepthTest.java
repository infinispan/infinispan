package org.infinispan.query.objectfilter.impl.ql.test;

import static org.assertj.core.api.Assertions.assertThat;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.infinispan.query.objectfilter.impl.ql.parse.IckleLexer;
import org.infinispan.query.objectfilter.impl.ql.parse.IckleParser;
import org.infinispan.query.objectfilter.impl.ql.parse.NestingDepthExceededException;
import org.testng.annotations.Test;

@Test(testName = "query.objectfilter.impl.ql.test.QueryParserNestingDepthTest", groups = "functional")
public class QueryParserNestingDepthTest {

   private static final String FROM = "FROM Note WHERE ";

   private static final int PAREN_DEPTH = 300;

   private static final int OVERFLOW_DEPTH = 10_000;

   public void testDeepParenthesesRejected() {
      assertRejectedWithNestingError(FROM + "(".repeat(PAREN_DEPTH) + "name = 'foo'" + ")".repeat(PAREN_DEPTH));
   }

   public void testEscapedApostropheDoesNotHideParentheses() {
      String query = FROM + "name = 'a\\'b' AND " + "(".repeat(PAREN_DEPTH) + "name = 'foo'" + ")".repeat(PAREN_DEPTH);
      assertRejectedWithNestingError(query);
   }

   public void testApostropheInDoubleQuotedStringDoesNotHideParentheses() {
      String query = FROM + "name = \"it's\" AND " + "(".repeat(PAREN_DEPTH) + "name = 'foo'" + ")".repeat(PAREN_DEPTH);
      assertRejectedWithNestingError(query);
   }

   public void testParenthesesInDoubleQuotedStringNotCounted() {
      assertParses(FROM + "name = \"" + "(".repeat(150) + "\"");
   }

   public void testParenthesesInRegexpLiteralNotCounted() {
      assertParses(FROM + "name : /" + "\\(".repeat(150) + "/");
   }

   public void testLongNotChain() {
      assertNoStackOverflow(FROM + "NOT ".repeat(OVERFLOW_DEPTH * 10) + "name = 'foo'");
   }

   public void testDeepPathIndexNesting() {
      String query = "FROM Note n WHERE n.a" + "[n.a".repeat(OVERFLOW_DEPTH) + "]".repeat(OVERFLOW_DEPTH) + " = 1";
      assertNoStackOverflow(query);
   }

   public void testNestingDepthWithinLimit() {
      assertParses(FROM + "(".repeat(100) + "name = 'foo'" + ")".repeat(100));
   }

   public void testNestingDepthExceedsLimit() {
      assertRejectedWithNestingError(FROM + "(".repeat(101) + "name = 'foo'" + ")".repeat(101));
   }

   public void testParenthesesInStringLiteralsNotCounted() {
      assertParses(FROM + "name = '" + "(".repeat(102) + "'");
   }

   private static String parse(String query) {
      try {
         IckleLexer lexer = new IckleLexer(new ANTLRStringStream(query));
         CommonTokenStream tokens = new CommonTokenStream(lexer);
         IckleParser parser = new IckleParser(tokens);
         parser.statement();
         if (parser.hasErrors()) {
            return "ParserError: " + parser.getErrorMessages();
         }
         return null;
      } catch (NestingDepthExceededException e) {
         return "NestingDepthExceeded: " + e.getMessage();
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
            .as("must succeed or fail with a nesting error, but got: %s", failure).isTrue();
   }

   private static void assertRejectedWithNestingError(String query) {
      assertThat(parse(query)).as("expected the nesting guard to reject the query").contains("NestingDepthExceeded");
   }
}
