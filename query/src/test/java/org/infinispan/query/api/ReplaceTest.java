package org.infinispan.query.api;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.api.ReplaceTest")
public class ReplaceTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .indexing()
            .enable()
            .addIndexedEntity(TestEntity.class)
            .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.HEAP)
            .addProperty(SearchConfig.ERROR_HANDLER, StaticTestingErrorHandler.class.getName());
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, cfg);
   }

   public void testReplaceSimple() {
      //for comparison we use a non-indexing cache here:
      EmbeddedCacheManager simpleCacheManager = TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, getDefaultStandaloneCacheConfig(true));
      try {
         Cache<Object, Object> simpleCache = simpleCacheManager.getCache();
         TestEntity se1 = new TestEntity("name1", "surname1", 10, "note");
         TestEntity se2 = new TestEntity("name2", "surname2", 10, "note"); // same id
         simpleCache.put(se1.getId(), se1);
         TestEntity se1ret = (TestEntity) simpleCache.replace(se2.getId(), se2);
         assertEquals(se1, se1ret);
      }
      finally {
         TestingUtil.killCacheManagers(simpleCacheManager);
      }
   }

   public void testReplaceSimpleSearchable() {
      TestEntity se1 = new TestEntity("name1", "surname1", 10, "note");
      TestEntity se2 = new TestEntity("name2", "surname2", 10, "note"); // same id
      cache.put(se1.getId(), se1);
      TestEntity se1ret = (TestEntity) cache.replace(se2.getId(), se2);
      assertEquals(se1, se1ret);
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testReplaceSimpleSearchableConditional() {
      TestEntity se1 = new TestEntity("name1", "surname1", 10, "note");
      TestEntity se2 = new TestEntity("name2", "surname2", 10, "note"); // same id
      cache.put(se1.getId(), se1);
      // note we use conditional replace here
      assert cache.replace(se2.getId(), se1, se2);
      StaticTestingErrorHandler.assertAllGood(cache);
   }

}
