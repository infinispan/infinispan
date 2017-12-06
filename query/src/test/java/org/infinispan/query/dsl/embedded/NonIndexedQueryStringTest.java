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
   public void testFullTextTerm() {
      super.testFullTextTerm();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextTermRightOperandAnalyzed() {
      super.testFullTextTermRightOperandAnalyzed();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextTermBoost() {
      super.testFullTextTermBoost();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextPhrase() {
      super.testFullTextPhrase();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextWithAggregation() {
      super.testFullTextWithAggregation();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextTermBoostAndSorting() {
      super.testFullTextTermBoostAndSorting();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextTermOccur() {
      super.testFullTextTermOccur();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextTermDoesntOccur() {
      super.testFullTextTermDoesntOccur();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028527: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed.")
   @Override
   public void testFullTextRangeWildcard() {
      super.testFullTextRangeWildcard();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028527: Full-text queries cannot be applied to property 'amount' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed.")
   @Override
   public void testFullTextRange() {
      super.testFullTextRange();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextPrefix() {
      super.testFullTextPrefix();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextWildcard() {
      super.testFullTextWildcard();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextWildcardFuzzyNotAllowed() {
      super.testFullTextWildcardFuzzyNotAllowed();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextFuzzy() {
      super.testFullTextFuzzy();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextFuzzyDefaultEdits() {
      super.testFullTextFuzzyDefaultEdits();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextFuzzySpecifiedEdits() {
      super.testFullTextFuzzySpecifiedEdits();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextRegexp() {
      super.testFullTextRegexp();
   }

   @Override
   public void testExactMatchOnAnalyzedFieldNotAllowed() {
      // Not applicable to non-indexed caches
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'description' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextTermOnNonAnalyzedFieldNotAllowed() {
      super.testFullTextTermOnNonAnalyzedFieldNotAllowed();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextRegexp2() {
      super.testFullTextRegexp2();
   }
}
