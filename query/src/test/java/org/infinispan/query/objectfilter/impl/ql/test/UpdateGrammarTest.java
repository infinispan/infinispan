package org.infinispan.query.objectfilter.impl.ql.test;

import org.testng.annotations.Test;

/**
 * Grammar-level tests for UPDATE statements in Ickle query language.
 */
@Test(testName = "query.objectfilter.impl.ql.test.UpdateGrammarTest", groups = "functional")
public class UpdateGrammarTest extends TestBase {

   @Test
   public void testUpdateSetScalar() {
      expectParserSuccess("update from example.Entity set name = 'newName'");
   }

   @Test
   public void testUpdateSetScalarWithWhere() {
      expectParserSuccess("update from example.Entity set name = 'newName' where id = 1");
   }

   @Test
   public void testUpdateSetMultipleFields() {
      expectParserSuccess("update from example.Entity set name = 'newName', set age = 25 where id = 1");
   }

   @Test
   public void testUpdateSetWithNamedParam() {
      expectParserSuccess("update from example.Entity set name = :newName where id = :id");
   }

   @Test
   public void testUpdateSetNull() {
      expectParserSuccess("update from example.Entity set description = null where id = 1");
   }

   @Test
   public void testUpdateSetBoolean() {
      expectParserSuccess("update from example.Entity set active = true where id = 1");
   }

   @Test
   public void testUpdateSetNestedField() {
      expectParserSuccess("update from example.Entity set address.city = 'Boston' where id = 1");
   }

   @Test
   public void testUpdateSetDeepNestedField() {
      expectParserSuccess("update from example.Entity set address.zip.code = '02101' where id = 1");
   }

   @Test
   public void testUpdateAddToCollection() {
      expectParserSuccess("update from example.Entity add tags = 'newTag' where id = 1");
   }

   @Test
   public void testUpdateAddMultipleToCollection() {
      expectParserSuccess("update from example.Entity add tags = ('tag1', 'tag2') where id = 1");
   }

   @Test
   public void testUpdateRemoveFromCollection() {
      expectParserSuccess("update from example.Entity remove tags = 'oldTag' where id = 1");
   }

   @Test
   public void testUpdateRemoveMultipleFromCollection() {
      expectParserSuccess("update from example.Entity remove tags = ('tag1', 'tag2') where id = 1");
   }

   @Test
   public void testUpdateSetCollectionReplace() {
      expectParserSuccess("update from example.Entity set tags = ['a', 'b', 'c'] where id = 1");
   }

   @Test
   public void testUpdateMixedOperations() {
      expectParserSuccess("update from example.Entity set name = 'x', add tags = 'y', remove labels = 'z' where id = 1");
   }

   @Test
   public void testUpdateWithoutWhere() {
      expectParserSuccess("update from example.Entity set name = 'x'");
   }

   @Test
   public void testUpdateNumericValue() {
      expectParserSuccess("update from example.Entity set count = 42 where active = true");
   }

   @Test
   public void testUpdateFloatingPointValue() {
      expectParserSuccess("update from example.Entity set weight = 3.14 where id = 1");
   }

   @Test
   public void testUpdateCaseInsensitive() {
      expectParserSuccess("UPDATE FROM example.Entity SET name = 'x' WHERE id = 1");
   }

   @Test
   public void testUpdateMissingSet() {
      expectParserFailure("update from example.Entity where id = 1");
   }

   @Test
   public void testUpdateMissingFrom() {
      expectParserFailure("update set name = 'x' where id = 1");
   }

   // --- Expression tests ---

   @Test
   public void testUpdateSetArithmeticAdd() {
      expectParserSuccess("update from example.Entity set count = count + 1 where id = 1");
   }

   @Test
   public void testUpdateSetArithmeticSubtract() {
      expectParserSuccess("update from example.Entity set count = count - 5 where id = 1");
   }

   @Test
   public void testUpdateSetArithmeticMultiply() {
      expectParserSuccess("update from example.Entity set price = price * 2 where id = 1");
   }

   @Test
   public void testUpdateSetArithmeticDivide() {
      expectParserSuccess("update from example.Entity set price = price / 2 where id = 1");
   }

   @Test
   public void testUpdateSetArithmeticModulo() {
      expectParserSuccess("update from example.Entity set count = count % 10 where id = 1");
   }

   @Test
   public void testUpdateSetComplexArithmetic() {
      expectParserSuccess("update from example.Entity set total = price * quantity + shipping where id = 1");
   }

   @Test
   public void testUpdateSetPrecedence() {
      expectParserSuccess("update from example.Entity set total = a + b * c where id = 1");
   }

   @Test
   public void testUpdateSetParenthesizedExpression() {
      expectParserSuccess("update from example.Entity set total = (a + b) * c where id = 1");
   }

   @Test
   public void testUpdateSetUnaryMinus() {
      expectParserSuccess("update from example.Entity set offset = -5 where id = 1");
   }

   @Test
   public void testUpdateSetNegatedProperty() {
      expectParserSuccess("update from example.Entity set offset = -value where id = 1");
   }

   @Test
   public void testUpdateSetFunctionCall() {
      expectParserSuccess("update from example.Entity set score = abs(score) where id = 1");
   }

   @Test
   public void testUpdateSetFunctionCallWithArgs() {
      expectParserSuccess("update from example.Entity set name = upper(name) where id = 1");
   }

   @Test
   public void testUpdateSetFunctionInExpression() {
      expectParserSuccess("update from example.Entity set total = round(price * quantity, 2) + 1 where id = 1");
   }

   @Test
   public void testUpdateSetStringConcat() {
      expectParserSuccess("update from example.Entity set fullName = firstName + ' ' + lastName where id = 1");
   }

   @Test
   public void testUpdateSetNestedPropertyRef() {
      expectParserSuccess("update from example.Entity set copy = source.value where id = 1");
   }

   @Test
   public void testUpdateSetExpressionWithParam() {
      expectParserSuccess("update from example.Entity set count = count + :increment where id = 1");
   }

   @Test
   public void testUpdateSetExpressionMultipleOps() {
      expectParserSuccess("update from example.Entity set a = a + 1, set b = b * 2 where id = 1");
   }

   @Test
   public void testUpdateSetAddExpressionToCollection() {
      expectParserSuccess("update from example.Entity add tags = concat('prefix_', id) where id = 1");
   }

   @Test
   public void testUpdateSetUnknownFunction() {
      // Syntactically valid; unknown function names are a semantic error caught by the visitor
      expectParserSuccess("update from example.Entity set x = nonexistfunc(1) where id = 1");
   }

   @Test
   public void testUpdateSetConstantDivision() {
      // a bare '/' between two numeric literals must lex as SLASH, not be swallowed
      expectParserSuccess("update from example.Entity set a = 1 / 2 where id = 1");
   }

   @Test
   public void testSecondDivisionInSameExpressionFailsToParse() {
      // ANTLR's max-munch lexes the second '/' of a single expression as a REGEXP_LITERAL
      // (price/2/quantity becomes price REGEXP_LITERAL(/2/) quantity), so at most one
      // division is possible per expression; spacing and parentheses do not change this.
      expectParserFailure("update from example.Entity set a = price/2/quantity where id = 1");
      expectParserFailure("update from example.Entity set a = price / 2 / quantity where id = 1");
      expectParserFailure("update from example.Entity set a = price / 2 + quantity / 3 where id = 1");
   }

   @Test
   public void testShortRegexpLiteralLexesAsRegexp() {
      // the max-munch behavior behind the one-division-per-expression limitation
      expectLexerSuccess("/2/", "REGEXP_LITERAL");
   }

   @Test
   public void testFullTextRegexpLiteralStillParses() {
      // a full-text /re/ term must still win (max-munch) over the new SLASH token
      expectParserSuccess("from example.Entity where name : /name/");
   }

   @Test
   public void testSlashLexesAsSlash() {
      expectLexerSuccess("/", "SLASH");
   }

   @Test
   public void testRegexpLiteralLexesAsRegexp() {
      expectLexerSuccess("/name/", "REGEXP_LITERAL");
   }
}
