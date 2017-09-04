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
   public void testFullTextTerm() throws Exception {
      super.testFullTextTerm();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextTermRightOperandAnalyzed() throws Exception {
      super.testFullTextTermRightOperandAnalyzed();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextTermBoost() throws Exception {
      super.testFullTextTermBoost();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextPhrase() throws Exception {
      super.testFullTextPhrase();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextWithAggregation() throws Exception {
      super.testFullTextWithAggregation();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextTermBoostAndSorting() throws Exception {
      super.testFullTextTermBoostAndSorting();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextTermOccur() throws Exception {
      super.testFullTextTermOccur();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextTermDoesntOccur() throws Exception {
      super.testFullTextTermDoesntOccur();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextRangeWildcard() throws Exception {
      super.testFullTextRangeWildcard();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextRange() throws Exception {
      super.testFullTextRange();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextPrefix() throws Exception {
      super.testFullTextPrefix();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextWildcard() throws Exception {
      super.testFullTextWildcard();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextWildcardFuzzyNotAllowed() throws Exception {
      super.testFullTextWildcardFuzzyNotAllowed();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextFuzzy() throws Exception {
      super.testFullTextFuzzy();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextFuzzyDefaultEdits() throws Exception {
      super.testFullTextFuzzyDefaultEdits();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextFuzzySpecifiedEdits() throws Exception {
      super.testFullTextFuzzySpecifiedEdits();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextRegexp() throws Exception {
      super.testFullTextRegexp();
   }

   @Test(enabled = false)
   public void testExactMatchOnAnalyzedFieldNotAllowed() throws Exception {
      // this test does not make sense in non-indexed mode
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN028521: Full-text queries cannot be applied to property 'description' in type sample_bank_account.Transaction unless the property is indexed and analyzed.")
   @Override
   public void testFullTextTermOnNonAnalyzedFieldNotAllowed() throws Exception {
      super.testFullTextTermOnNonAnalyzedFieldNotAllowed();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextRegexp2() throws Exception {
      super.testFullTextRegexp2();
   }

   @Test(enabled = false)
   public void testCustomFieldAnalyzer() {
      //not working with non-indexed caches
   }
}
