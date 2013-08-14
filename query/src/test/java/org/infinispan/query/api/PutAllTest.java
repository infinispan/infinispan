package org.infinispan.query.api;

import org.apache.lucene.search.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

@Test(groups = "functional", testName = "query.api.PutAllTest")
@CleanupAfterMethod
public class PutAllTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.indexing()
            .enable()
            .indexLocalOnly(false)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testOverwriteNotIndexedValue() {
      final long id = 10;

      cache.put(id, new NotIndexedType("name1"));

      Map<Object, Object> map = new HashMap<Object, Object>();
      map.put(id, new TestEntity("name2", "surname2", id, "note"));
      cache.putAll(map);

      CacheQuery q1 = queryByNameField("name2", AnotherTestEntity.class);
      assertEquals(1, q1.getResultSize());
      assertEquals(TestEntity.class, q1.list().get(0).getClass());
   }

   public void testAsyncOverwriteNotIndexedValue() throws Exception {
      final long id = 10;

      cache.put(id, new NotIndexedType("name1"));

      Map<Object, Object> map = new HashMap<Object, Object>();
      map.put(id, new TestEntity("name2", "surname2", id, "note"));
      Future futureTask = cache.putAllAsync(map);
      futureTask.get();
      assertTrue(futureTask.isDone());
      CacheQuery q1 = queryByNameField("name2", AnotherTestEntity.class);
      assertEquals(1, q1.getResultSize());
      assertEquals(TestEntity.class, q1.list().get(0).getClass());
   }

   public void testOverwriteWithNonIndexedValue() {
      final long id = 10;

      cache.put(id, new TestEntity("name1", "surname1", id, "note"));
      CacheQuery q1 = queryByNameField("name1", TestEntity.class);
      assertEquals(1, q1.getResultSize());
      assertEquals(TestEntity.class, q1.list().get(0).getClass());

      Map<Object, Object> map = new HashMap<Object, Object>();
      map.put(id, new NotIndexedType("name2"));
      cache.putAll(map);

      CacheQuery q2 = queryByNameField("name1", TestEntity.class);
      assertEquals(0, q2.getResultSize());

      CacheQuery q3 = queryByNameField("name2", TestEntity.class);
      assertEquals(0, q3.getResultSize());
   }

   public void testAsyncOverwriteWithNonIndexedValue() throws Exception {
      final long id = 10;

      cache.put(id, new TestEntity("name1", "surname1", id, "note"));
      CacheQuery q1 = queryByNameField("name1", TestEntity.class);
      assertEquals(1, q1.getResultSize());
      assertEquals(TestEntity.class, q1.list().get(0).getClass());

      Map<Object, Object> map = new HashMap<Object, Object>();
      map.put(id, new NotIndexedType("name2"));
      Future futureTask = cache.putAllAsync(map);
      futureTask.get();
      assertTrue(futureTask.isDone());
      CacheQuery q2 = queryByNameField("name1", TestEntity.class);
      assertEquals(0, q2.getResultSize());

      CacheQuery q3 = queryByNameField("name2", TestEntity.class);
      assertEquals(0, q3.getResultSize());
   }

   public void testOverwriteIndexedValue() {
      final long id = 10;

      cache.put(id, new TestEntity("name1", "surname1", id, "note"));

      CacheQuery q1 = queryByNameField("name1", TestEntity.class);
      assertEquals(1, q1.getResultSize());
      assertEquals(TestEntity.class, q1.list().get(0).getClass());

      Map<Object, Object> map = new HashMap<Object, Object>();
      map.put(id, new AnotherTestEntity("name2"));
      cache.putAll(map);

      CacheQuery q2 = queryByNameField("name1", TestEntity.class);
      assertEquals(0, q2.getResultSize());

      CacheQuery q3 = queryByNameField("name2", AnotherTestEntity.class);
      assertEquals(1, q3.getResultSize());
      assertEquals(AnotherTestEntity.class, q3.list().get(0).getClass());
   }

   private CacheQuery queryByNameField(String name, Class<?> entity) {
      SearchManager sm = Search.getSearchManager(cache);
      Query query = sm.buildQueryBuilderForClass(entity)
            .get().keyword().onField("name").matching(name).createQuery();
      return sm.getQuery(query);
   }
}
