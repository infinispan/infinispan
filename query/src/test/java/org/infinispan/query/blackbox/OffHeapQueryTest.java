package org.infinispan.query.blackbox;

import static org.infinispan.query.helper.TestQueryHelperFactory.createCacheQuery;
import static org.infinispan.query.helper.TestQueryHelperFactory.extractSearchFactory;
import static org.testng.Assert.assertEquals;

import org.apache.lucene.index.IndexReader;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = {"functional"}, testName = "query.blackbox.OffHeapQueryTest")
public class OffHeapQueryTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(false);
      cfg.memory().storageType(StorageType.OFF_HEAP).size(10);
      cfg.indexing().index(Index.ALL)
            .addIndexedEntity(Person.class)
            .addProperty("default.directory_provider", "local-heap");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Test
   public void testQuery() throws Exception {
      cache.put("1", new Person("Donald", "MAGA", 78));

      assertEquals(getIndexDocs(), 1);

      CacheQuery<Object> queryFromLucene = createCacheQuery(cache, "name", "Donald");
      assertEquals(1, queryFromLucene.list().size());

      Query queryFromIckle = Search.getQueryFactory(cache)
            .create("From org.infinispan.query.test.Person p where p.name:'Donald'");
      assertEquals(1, queryFromIckle.list().size());
   }

   private int getIndexDocs() {
      SearchIntegrator searchIntegrator = extractSearchFactory(cache);
      IndexReader indexReader = searchIntegrator.getIndexManager("person")
            .getReaderProvider().openIndexReader();
      return indexReader.numDocs();
   }
}
