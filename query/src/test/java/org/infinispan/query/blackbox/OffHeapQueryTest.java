package org.infinispan.query.blackbox;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.query.helper.TestQueryHelperFactory.createCacheQuery;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.helper.IndexAccessor;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = {"functional"}, testName = "query.blackbox.OffHeapQueryTest")
public class OffHeapQueryTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(false);
      cfg.memory().storage(StorageType.OFF_HEAP).maxCount(10);
      cfg.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class);
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, cfg);
   }

   @Test
   public void testQuery() throws Exception {
      cache.put("1", new Person("Donald", "MAGA", 78));

      assertEquals(1, getIndexDocs());

      Query<Object> queryFromLucene = createCacheQuery(Person.class, cache, "name", "Donald");
      assertEquals(1, queryFromLucene.execute().list().size());

      Query<Object> queryFromIckle = cache.query("From org.infinispan.query.test.Person p where p.name:'Donald'");
      assertEquals(1, queryFromIckle.execute().list().size());
   }

   private long getIndexDocs() {
     return IndexAccessor.of(cache, Person.class).getIndexReader().numDocs();
   }
}
