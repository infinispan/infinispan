package org.infinispan.client.hotrod.query;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Test for query language in remote mode.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteNonIndexedQueryStringTest")
public class RemoteNonIndexedQueryStringTest extends RemoteQueryStringTest {

   protected ConfigurationBuilder getConfigurationBuilder() {
      return hotRodCacheConfiguration();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextTerm() {
      super.testFullTextTerm();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextTermRightOperandAnalyzed() {
      super.testFullTextTermRightOperandAnalyzed();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextTermBoost() {
      super.testFullTextTermBoost();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextPhrase() {
      super.testFullTextPhrase();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextWithAggregation() {
      super.testFullTextWithAggregation();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextTermBoostAndSorting() {
      super.testFullTextTermBoostAndSorting();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextTermOccur() {
      super.testFullTextTermOccur();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextTermDoesntOccur() {
      super.testFullTextTermDoesntOccur();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextRangeWildcard() {
      super.testFullTextRangeWildcard();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextRange() {
      super.testFullTextRange();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextPrefix() {
      super.testFullTextPrefix();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextWildcard() {
      super.testFullTextWildcard();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextWildcardFuzzyNotAllowed() {
      super.testFullTextWildcardFuzzyNotAllowed();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextFuzzy() {
      super.testFullTextFuzzy();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextFuzzyDefaultEdits() {
      super.testFullTextFuzzyDefaultEdits();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextFuzzySpecifiedEdits() {
      super.testFullTextFuzzySpecifiedEdits();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextRegexp() {
      super.testFullTextRegexp();
   }

   @Override
   public void testExactMatchOnAnalyzedFieldNotAllowed() {
      // Not applicable to non-indexed caches
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN028521: Full-text queries cannot be applied to property 'description' in type sample_bank_account.Transaction unless the property is indexed and analyzed.")
   @Override
   public void testFullTextTermOnNonAnalyzedFieldNotAllowed() {
      super.testFullTextTermOnNonAnalyzedFieldNotAllowed();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextRegexp2() {
      super.testFullTextRegexp2();
   }

   @Override
   public void testCustomFieldAnalyzer() {
      // Not applicable to non-indexed caches
   }
}
