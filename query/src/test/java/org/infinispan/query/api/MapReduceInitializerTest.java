package org.infinispan.query.api;

import org.hibernate.search.backend.LuceneWork;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.query.impl.massindex.IndexingMapper;
import org.infinispan.query.impl.massindex.IndexingReducer;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.Map;

/**
 * Testing the MapReduceInitializer class with simple cases.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.api.MapReduceInitializerTest")
public class MapReduceInitializerTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cacheCfg.indexing()
            .enable()
            .indexLocalOnly(true)
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("default.exclusive_index_use", "false")
            .addProperty("lucene_version", "LUCENE_36");
      cacheCfg.clustering().stateTransfer().fetchInMemoryState(true);

      createClusteredCaches(2, cacheCfg);
   }

   public void testInitMapper() throws InterruptedException {
      cache(0).put("key1", new Car("ford", "blue", 160));
      cache(1).put("key2", new Car("bmw", "black", 160));
      cache(0).put("key3", new Car("mersedes", "white", 160));

      Map result = new MapReduceTask<Object, Object, Object, LuceneWork>(cache(0))
            .mappedWith(new SomeMapper())
            .reducedWith(new IndexingReducer())
            .execute();

      Assert.assertTrue(result.isEmpty());
   }

   public void testInitReducer () throws InterruptedException {
      cache(0).put("key1", new Car("ford", "blue", 160));
      cache(1).put("key2", new Car("bmw", "black", 160));
      cache(0).put("key3", new Car("mersedes", "white", 160));

      Map result = new MapReduceTask<Object, Object, Object, LuceneWork>(cache(0))
            .mappedWith(new IndexingMapper())
            .reducedWith(new SomeReducer())
            .execute();

      Assert.assertFalse(result.isEmpty());
   }

   private static class SomeReducer implements Reducer<Object, LuceneWork> {
      @Override
      public LuceneWork reduce(Object reducedKey, Iterator<LuceneWork> iter) {
         return iter.next();
      }
   }

   private static class SomeMapper implements Mapper<Object, Object, Object, LuceneWork> {
      @Override
      public void map(Object key, Object value, Collector<Object, LuceneWork> collector) {
         System.out.println("Some stuff to do.");
      }
   }
}
