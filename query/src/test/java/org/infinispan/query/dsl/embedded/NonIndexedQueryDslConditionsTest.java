package org.infinispan.query.dsl.embedded;

import org.hibernate.hql.ParsingException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Test for query conditions (filtering) on cache without indexing. Exercises the whole query DSL on the sample domain
 * model.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(groups = "functional", testName = "query.dsl.NonIndexedQueryDslConditionsTest")
public class NonIndexedQueryDslConditionsTest extends QueryDslConditionsTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Override
   protected QueryFactory getQueryFactory() {
      return Search.getQueryFactory(cache);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Indexing was not enabled on this cache.*")
   @Override
   public void testIndexPresence() {
      Search.getSearchManager(cache).getSearchFactory();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN000405:.*")
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
