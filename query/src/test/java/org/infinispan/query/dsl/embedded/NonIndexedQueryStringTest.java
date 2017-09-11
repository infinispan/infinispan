package org.infinispan.query.dsl.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Test string-based queries on a not indexed cache.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
@Test(groups = {"functional", "smoke"}, testName = "query.dsl.embedded.NonIndexedQueryStringTest")
public class NonIndexedQueryStringTest extends QueryStringTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      createClusteredCaches(1, cfg);
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextTerm() throws Exception {
      super.testFullTextTerm();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextTermRightOperandAnalyzed() throws Exception {
      super.testFullTextTermRightOperandAnalyzed();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextTermBoost() throws Exception {
      super.testFullTextTermBoost();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextPhrase() throws Exception {
      super.testFullTextPhrase();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextWithAggregation() throws Exception {
      super.testFullTextWithAggregation();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextTermBoostAndSorting() throws Exception {
      super.testFullTextTermBoostAndSorting();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextTermOccur() throws Exception {
      super.testFullTextTermOccur();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextTermDoesntOccur() throws Exception {
      super.testFullTextTermDoesntOccur();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028527: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed.")
   @Override
   public void testFullTextRangeWildcard() throws Exception {
      super.testFullTextRangeWildcard();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028527: Full-text queries cannot be applied to property 'amount' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed.")
   @Override
   public void testFullTextRange() throws Exception {
      super.testFullTextRange();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextPrefix() throws Exception {
      super.testFullTextPrefix();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextWildcard() throws Exception {
      super.testFullTextWildcard();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextWildcardFuzzyNotAllowed() throws Exception {
      super.testFullTextWildcardFuzzyNotAllowed();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextFuzzy() throws Exception {
      super.testFullTextFuzzy();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextFuzzyDefaultEdits() throws Exception {
      super.testFullTextFuzzyDefaultEdits();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextFuzzySpecifiedEdits() throws Exception {
      super.testFullTextFuzzySpecifiedEdits();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextRegexp() throws Exception {
      super.testFullTextRegexp();
   }

   @Test(enabled = false, description = "Not applicable in non-indexed mode")
   public void testExactMatchOnAnalyzedFieldNotAllowed() throws Exception {
      // this test does not make sense in non-indexed mode
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'description' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextTermOnNonAnalyzedFieldNotAllowed() throws Exception {
      super.testFullTextTermOnNonAnalyzedFieldNotAllowed();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextRegexp2() throws Exception {
      super.testFullTextRegexp2();
   }
}
