package org.infinispan.query.distributed;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import javax.transaction.TransactionManager;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Similar to MultiNodeDistributedTest, but using a local cache configuration both for
 * the indexed cache and for the storage of the index data.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.distributed.IndexManagerLocalTest")
public class IndexManagerLocalTest extends SingleCacheManagerTest {

   protected EmbeddedCacheManager createCacheManager() throws IOException {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
            .clustering()
            .cacheMode(CacheMode.LOCAL)
            .indexing()
            .index(Index.ALL)
            .addProperty("hibernate.search.lucene_version", "LUCENE_CURRENT")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager");

      if(transactionsEnabled()) {
         builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   protected boolean transactionsEnabled() {
      return false;
   }

   public void testIndexingWork() throws Exception {
      assertIndexSize(0);
      store("k1", new Person("K. Firt", "Is not a character from the matrix", 1));
      assertIndexSize(1);
      store("k2", new Person("K. Seycond", "Is a pilot", 1));
      assertIndexSize(2);
   }

   protected void store(String key, Person person) throws Exception {
      TransactionManager transactionManager = cache.getAdvancedCache().getTransactionManager();
      if (transactionsEnabled()) transactionManager.begin();
      cache.put(key, person);
      if (transactionsEnabled()) transactionManager.commit();
   }

   protected void assertIndexSize(int expectedIndexSize) {
      SearchManager searchManager = Search.getSearchManager(cache);
      CacheQuery query = searchManager.getQuery(new MatchAllDocsQuery(), Person.class);
      assertEquals(expectedIndexSize, query.list().size());
      StaticTestingErrorHandler.assertAllGood(cache);
   }

}
