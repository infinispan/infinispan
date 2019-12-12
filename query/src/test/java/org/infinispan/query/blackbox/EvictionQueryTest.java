package org.infinispan.query.blackbox;

import static org.infinispan.query.helper.TestQueryHelperFactory.createCacheQuery;
import static org.infinispan.query.helper.TestQueryHelperFactory.extractSearchFactory;
import static org.testng.Assert.assertEquals;

import org.apache.lucene.index.IndexReader;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = {"functional"}, testName = "query.blackbox.OffHeapQueryTest")
public class EvictionQueryTest extends SingleCacheManagerTest {

   private StorageType storageType;
   private EvictionType evictionType;

   @Override
   protected String parameters() {
      return "[" + storageType + ", " + evictionType + "]";
   }

   @Factory
   public Object[] factory() {
      return new Object[] {
            new EvictionQueryTest().storageType(StorageType.OFF_HEAP).evictionType(EvictionType.COUNT),
            new EvictionQueryTest().storageType(StorageType.BINARY).evictionType(EvictionType.COUNT),
            new EvictionQueryTest().storageType(StorageType.OBJECT).evictionType(EvictionType.COUNT),
            new EvictionQueryTest().storageType(StorageType.OFF_HEAP).evictionType(EvictionType.MEMORY),
            new EvictionQueryTest().storageType(StorageType.BINARY).evictionType(EvictionType.MEMORY),
      };
   }

   EvictionQueryTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   EvictionQueryTest evictionType(EvictionType evictionType) {
      this.evictionType = evictionType;
      return this;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(false);
      cfg.memory()
            .storageType(storageType)
            .evictionType(evictionType)
            .size(10_000);
      cfg.indexing().index(Index.ALL)
            .addIndexedEntity(Person.class)
            .addProperty("default.directory_provider", "local-heap");
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, cfg);
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
