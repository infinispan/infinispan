package org.infinispan.query.blackbox;

import static org.testng.AssertJUnit.assertEquals;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.helper.SearchConfig;
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
            .enable()
            .addIndexedEntity(ClassWithLongIndexName.class)
            .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.HEAP);
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
      Query cq = Search.getQueryFactory(cache2).create(q);
      assertEquals(100, cq.execute().hitCount().orElse(-1));

      addClusterEnabledCacheManager(SCI.INSTANCE, getDefaultConfiguration());
      TestingUtil.waitForNoRebalance(cache(0), cache(1), cache(2), cache(3));

      cq = Search.getQueryFactory(cache(3)).create(q);
      assertEquals(100, cq.execute().hitCount().orElse(-1));
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

      @Field(store = Store.YES)
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
