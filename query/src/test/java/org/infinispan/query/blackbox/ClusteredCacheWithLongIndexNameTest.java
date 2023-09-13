package org.infinispan.query.blackbox;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * The test verifies the issue ISPN-3092.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCacheWithLongIndexNameTest")
@CleanupAfterMethod
public class ClusteredCacheWithLongIndexNameTest extends MultipleCacheManagersTest {

   private Cache<String, ClassWithLongIndexName> cache0, cache1, cache2;

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(3, SCI.INSTANCE, getDefaultConfiguration());
      cache0 = cache(0);
      cache1 = cache(1);
      cache2 = cache(2);
   }

   private ConfigurationBuilder getDefaultConfiguration() {
      ConfigurationBuilder cacheCfg = TestCacheManagerFactory.getDefaultCacheConfiguration(transactionsEnabled(), false);
      cacheCfg.
            clustering()
            .cacheMode(getCacheMode())
            .indexing()
            .storage(LOCAL_HEAP)
            .enable()
            .addIndexedEntity(ClassWithLongIndexName.class);
      return cacheCfg;
   }

   public boolean transactionsEnabled() {
      return false;
   }

   public CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   public void testAdditionOfNewNode() {
      for (int i = 0; i < 100; i++) {
         cache0.put("key" + i, new ClassWithLongIndexName("value" + i));
      }

      String q = String.format("FROM %s WHERE name:'value*'", ClassWithLongIndexName.class.getName());
      Query cq = cache2.query(q);
      assertEquals(100, cq.execute().count().value());

      addClusterEnabledCacheManager(SCI.INSTANCE, getDefaultConfiguration());
      TestingUtil.waitForNoRebalance(cache(0), cache(1), cache(2), cache(3));

      cq = cache(3).query(q);
      assertEquals(100, cq.execute().count().value());
   }

   // index name as in bug description
   @Indexed(index = "default_taskworker-java__com.google.appengine.api.datastore.Entity")
   public static class ClassWithLongIndexName {

      @ProtoField(1)
      String name;

      @ProtoFactory
      ClassWithLongIndexName(String name) {
         this.name = name;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         ClassWithLongIndexName that = (ClassWithLongIndexName) o;
         return name != null ? name.equals(that.name) : that.name == null;
      }

      @Text
      public String getName() {
         return name;
      }

      @Override
      public int hashCode() {
         return name != null ? name.hashCode() : 0;
      }

      @Override
      public String toString() {
         return "ClassWithLongIndexName{name='" + name + "'}";
      }
   }

   @AutoProtoSchemaBuilder(
         includeClasses = ClassWithLongIndexName.class,
         schemaFileName = "test.query.blackbox.ClusteredCacheWithLongIndexNameTest.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.ClusteredCacheWithLongIndexNameTest",
         service = false
   )
   interface SCI extends SerializationContextInitializer {
      SCI INSTANCE = new SCIImpl();
   }
}
