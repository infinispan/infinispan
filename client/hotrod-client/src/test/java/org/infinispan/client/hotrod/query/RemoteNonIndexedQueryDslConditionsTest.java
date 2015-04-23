package org.infinispan.client.hotrod.query;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

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
      org.infinispan.query.Search.getSearchManager(getEmbeddedCache()).unwrap(SearchIntegrator.class);
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN000405:.*")
   @Override
   public void testInvalidEmbeddedAttributeQuery() throws Exception {
      super.testInvalidEmbeddedAttributeQuery();
   }

   @Test
   @Override
   public void testNullOnIntegerField() throws Exception {
      super.testNullOnIntegerField();
   }
}
