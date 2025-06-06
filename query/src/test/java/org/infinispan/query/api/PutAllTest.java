package org.infinispan.query.api;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.api.PutAllTest")
@CleanupAfterMethod
public class PutAllTest extends SingleCacheManagerTest {

   private StorageType storageType;

   @Override
   protected String parameters() {
      return "[" + storageType + "]";
   }

   @Factory
   public Object[] factory() {
      return new Object[]{
            new PutAllTest().storageType(StorageType.OFF_HEAP),
            new PutAllTest().storageType(StorageType.HEAP),
      };
   }

   PutAllTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.indexing()
            .enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(TestEntity.class)
            .addIndexedEntity(AnotherTestEntity.class)
            .writer().queueSize(5).queueCount(100);
      cfg.memory()
            .storage(storageType);
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, cfg);
   }

   public void testOverwriteNotIndexedValue() {
      final long id = 10;

      cache.put(id, new NotIndexedType("name1"));

      Map<Object, Object> map = new HashMap<>();
      map.put(id, new TestEntity("name2", "surname2", id, "note"));
      cache.putAll(map);

      Query<AnotherTestEntity> q1 = queryByNameField("name2", AnotherTestEntity.class);
      Query<TestEntity> q2 = queryByNameField("name2", TestEntity.class);
      assertEquals(1, q1.execute().count().value() + q2.execute().count().value());
      assertEquals(TestEntity.class, q2.list().get(0).getClass());
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testAsyncOverwriteNotIndexedValue() throws Exception {
      final long id = 10;

      cache.put(id, new NotIndexedType("name1"));

      Map<Object, Object> map = new HashMap<>();
      map.put(id, new TestEntity("name2", "surname2", id, "note"));
      Future<?> futureTask = cache.putAllAsync(map);
      futureTask.get();
      assertTrue(futureTask.isDone());
      Query<TestEntity> q1 = queryByNameField("name2", TestEntity.class);
      Query<AnotherTestEntity> q2 = queryByNameField("name2", AnotherTestEntity.class);
      assertEquals(1, q1.execute().count().value() + q2.execute().count().value());
      assertEquals(TestEntity.class, q1.list().get(0).getClass());
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testOverwriteWithNonIndexedValue() {
      final long id = 10;

      cache.put(id, new TestEntity("name1", "surname1", id, "note"));
      Query<TestEntity> q1 = queryByNameField("name1", TestEntity.class);
      Query<?> q2 = queryByNameField("name1", AnotherTestEntity.class);
      assertEquals(1, q1.execute().count().value() + q2.execute().count().value());
      assertEquals(TestEntity.class, q1.list().get(0).getClass());

      Map<Object, Object> map = new HashMap<>();
      map.put(id, new NotIndexedType("name2"));
      cache.putAll(map);

      q2 = queryByNameField("name1", TestEntity.class);
      assertEquals(0, q2.execute().count().value());

      Query<?> q3 = queryByNameField("name2", TestEntity.class);
      assertEquals(0, q3.execute().count().value());
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testAsyncOverwriteWithNonIndexedValue() throws Exception {
      final long id = 10;

      cache.put(id, new TestEntity("name1", "surname1", id, "note"));
      Query<?> q1 = queryByNameField("name1", TestEntity.class);
      assertEquals(1, q1.execute().count().value());
      assertEquals(TestEntity.class, q1.list().get(0).getClass());

      Map<Object, Object> map = new HashMap<>();
      map.put(id, new NotIndexedType("name2"));
      Future<?> futureTask = cache.putAllAsync(map);
      futureTask.get();
      assertTrue(futureTask.isDone());
      Query<TestEntity> q2 = queryByNameField("name1", TestEntity.class);
      assertEquals(0, q2.execute().count().value());

      Query<TestEntity> q3 = queryByNameField("name2", TestEntity.class);
      assertEquals(0, q3.execute().count().value());
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testOverwriteIndexedValue() {
      final long id = 10;

      cache.put(id, new TestEntity("name1", "surname1", id, "note"));

      Query<TestEntity> q1 = queryByNameField("name1", TestEntity.class);
      assertEquals(1, q1.execute().count().value());
      assertEquals(TestEntity.class, q1.list().get(0).getClass());

      Map<Object, Object> map = new HashMap<>();
      map.put(id, new AnotherTestEntity("name2"));
      cache.putAll(map);

      Query<TestEntity> q2 = queryByNameField("name1", TestEntity.class);
      assertEquals(0, q2.execute().count().value());

      Query<AnotherTestEntity> q3 = queryByNameField("name2", AnotherTestEntity.class);
      assertEquals(1, q3.execute().count().value());
      assertEquals(AnotherTestEntity.class, q3.list().get(0).getClass());
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test
   @TestForIssue(jiraKey = "ISPN-14115")
   public void testOverwriteIndexedValue_heavyLoad() {
      for (int i=0; i<1000; i++) {
         Map<Object, Object> map = new HashMap<>();
         map.put(1, new TestEntity("name1", "surname1", 1, "note"));
         map.put(2, new AnotherTestEntity("name2"));
         cache.putAll(map);

         map.clear();
         map.put(1, new AnotherTestEntity("name2"));
         map.put(2, new TestEntity("name1", "surname1", 1, "note"));
         cache.putAll(map);
      }
   }

   private <T> Query<T> queryByNameField(String name, Class<T> entity) {
      return cache.query(String.format("FROM %s WHERE name = '%s'", entity.getName(), name));
   }
}
