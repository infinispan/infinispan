package org.infinispan.client.hotrod.query;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.query.RemoteNonIndexedQueryConditionsTest")
public class RemoteNonIndexedQueryConditionsTest extends RemoteQueryConditionsTest {

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      return hotRodCacheConfiguration();
   }

   @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Indexing was not enabled on cache.*")
   @Override
   public void testIndexPresence() {
      org.infinispan.query.Search.getIndexer(getEmbeddedCache());
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text or spatial queries.")
   @Override
   public void testFullTextTerm() {
      super.testFullTextTerm();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: The cache must be indexed in order to use full-text or spatial queries.")
   @Override
   public void testFullTextPhrase() {
      super.testFullTextPhrase();
   }
}
