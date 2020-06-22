package org.infinispan.query.nulls;

import static org.infinispan.test.TestingUtil.withTx;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * @author Anna Manukyan
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.nulls.NullCollectionElementsClusteredTest")
public class NullCollectionElementsClusteredTest extends MultipleCacheManagersTest {

   private Cache<String, Foo> cache1, cache2;

   public void testQuerySkipsNullsInList() throws Exception {
      prepareData();
      withTx(tm(1), (Callable<Void>) () -> {
         cache2.remove("2");   // cache will now be out of sync with the index

         // Query a cache where key "2" is not present in the index
         Cache<String, Foo> queryCache = getNonOwner("2");

         List<?> list = createLocalCacheQuery(queryCache, "2").execute().list();
         assertEquals("Wrong list size.", 0, list.size());
         return null;
      });
   }

   public void testQuerySkipsNullsInEagerIterator() throws Exception {
      prepareData();

      withTx(tm(0), (Callable<Void>) () -> {
         cache1.remove("1");   // cache will now be out of sync with the index

         CloseableIterator<?> iterator = createLocalCacheQuery(cache1, "1").iterator();
         assertFalse("Iterator should not have elements.", iterator.hasNext());
         try {
            iterator.next();
            fail("Expected NoSuchElementException");
         } catch (NoSuchElementException e) {
            // pass
         }
         return null;
      });
   }

   // This is the same as the verification above, only verifying the default iterator() method.
   public void testQuerySkipsNullsInDefaultIterator() throws Exception {
      prepareData();

      withTx(tm(0), (Callable<Void>) () -> {
         Cache<String, Foo> cache = getKeyLocation("1");
         cache.remove("1");   // cache will now be out of sync with the index

         CloseableIterator<?> iterator = createLocalCacheQuery(cache, "1").iterator();
         assertFalse(iterator.hasNext());
         try {
            iterator.next();
            fail("Expected NoSuchElementException");
         } catch (NoSuchElementException e) {
            // pass
         }
         return null;
      });
   }

   // This is the same as the verification above, only verifying the iterating using cacheQuery object.
   public void testQuerySkipsNullsInCacheQueryIterator() throws Exception {
      prepareData();

      withTx(tm(0), (Callable<Void>) () -> {
         Cache<String, Foo> cache = getKeyLocation("1");
         cache.remove("1");   // cache will now be out of sync with the index

         Query<?> cacheQuery = createLocalCacheQuery(cache, "1");
         //pruivo: the result size includes the null elements...
         assertEquals("Wrong result size.", 1, cacheQuery.execute().hitCount().orElse(-1));

         for (Object obj : cacheQuery) {
            fail("The iterator should not contain any data but it contains " + obj);
         }

         return null;
      });
   }

   public void testQuerySkipsNullsInLazyIterator() throws Exception {
      prepareData();

      withTx(tm(1), (Callable<Void>) () -> {
         cache2.remove("2");   // cache will now be out of sync with the index
         // Query a cache where key "2" is present in the index
         Cache<?, ?> queryCache = getKeyLocation("2").equals(cache1) ? cache2 : cache1;

         Iterator<?> iterator = createLocalCacheQuery(queryCache, "2").iterator();
         assertFalse(iterator.hasNext());
         try {
            iterator.next();
            fail("Expected NoSuchElementException");
         } catch (NoSuchElementException e) {
            // pass
         }
         return null;
      });
   }

   public void testQueryReturnsNullWhenProjectingCacheValue() throws Exception {
      prepareData();

      withTx(tm(0), (Callable<Void>) () -> {
         Cache<String, Foo> cache = getKeyLocation("1");
         cache.remove("1");   // cache will now be out of sync with the index
         QueryFactory queryFactory = Search.getQueryFactory(cache);

         String q = String.format("SELECT bar FROM %s where bar:'1'", Foo.class.getName());
         Query<Object[]> cacheQuery = queryFactory.create(q);

         CloseableIterator<Object[]> iterator = cacheQuery.iterator();
         assertTrue("Expected an element in iterator.", iterator.hasNext());
         Object[] projection = iterator.next();
         assertEquals("1", projection[0]);
         return null;
      });
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cfg.clustering().hash().numOwners(1);
      cfg
            .indexing()
            .enable()
            .addIndexedEntity(Foo.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      createClusteredCaches(2, SCI.INSTANCE, cfg);

      cache1 = cache(0);
      cache2 = cache(1);
   }

   private void prepareData() throws Exception {
      withTx(tm(0), (Callable<Void>) () -> {
         cache1.put("1", new Foo("1"));
         return null;
      });

      withTx(tm(1), (Callable<Void>) () -> {
         cache2.put("2", new Foo("2"));
         return null;
      });
   }

   private Query<Foo> createLocalCacheQuery(Cache<?, ?> cache, String value) {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      String q = String.format("FROM %s where bar:'%s'", Foo.class.getName(), value);
      return queryFactory.create(q, IndexedQueryMode.FETCH);
   }

   private Cache<String, Foo> getKeyLocation(String key) {
      Address cache1Address = cache1.getAdvancedCache().getRpcManager().getAddress();
      LocalizedCacheTopology cacheTopology = cache1.getAdvancedCache().getDistributionManager().getCacheTopology();
      DistributionInfo distribution = cacheTopology.getDistribution(key);
      Address primary = distribution.primary();
      return primary.equals(cache1Address) ? cache1 : cache2;
   }

   private Cache<String, Foo> getNonOwner(String key) {
      LocalizedCacheTopology cacheTopology = cache1.getAdvancedCache().getDistributionManager().getCacheTopology();
      DistributionInfo distribution = cacheTopology.getDistribution(key);
      List<Address> addresses = distribution.writeOwners();
      List<Cache<String, Foo>> allCaches = caches();
      return allCaches.stream()
            .filter(c -> !addresses.contains(c.getAdvancedCache().getRpcManager().getAddress()))
            .findFirst().get();
   }

   @Indexed(index = "FooIndex")
   public static class Foo implements Serializable {
      private String bar;

      @ProtoFactory
      Foo(String bar) {
         this.bar = bar;
      }

      @Field(name = "bar", store = Store.YES)
      @ProtoField(number = 1)
      public String getBar() {
         return bar;
      }
   }

   @AutoProtoSchemaBuilder(
         includeClasses = Foo.class,
         schemaFileName = "test.query.nulls.NullCollectionElementsClusteredTest.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.NullCollectionElementsClusteredTest",
         service = false
   )
   interface SCI extends SerializationContextInitializer {
      SCI INSTANCE = new SCIImpl();
   }
}
