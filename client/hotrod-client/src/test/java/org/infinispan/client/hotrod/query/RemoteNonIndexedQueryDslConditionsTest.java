package org.infinispan.client.hotrod.query;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Test for query conditions (filtering) without an index. Exercises the whole query DSL on the sample domain model.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteNonIndexedQueryDslConditionsTest")
public class RemoteNonIndexedQueryDslConditionsTest extends RemoteQueryDslConditionsTest {

   protected ConfigurationBuilder getConfigurationBuilder() {
      return hotRodCacheConfiguration();
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Indexing was not enabled on this cache.*")
   @Override
   public void testIndexPresence() {
      org.infinispan.query.Search.getSearchManager(getEmbeddedCache());
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextTerm() {
      super.testFullTextTerm();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text queries.")
   @Override
   public void testFullTextPhrase() {
      super.testFullTextPhrase();
   }
}
