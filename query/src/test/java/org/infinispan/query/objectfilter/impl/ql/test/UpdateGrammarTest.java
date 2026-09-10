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
}
