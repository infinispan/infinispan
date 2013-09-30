package org.infinispan.query.nulls;

import org.apache.lucene.search.Query;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ProjectionConstants;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

import static org.infinispan.query.FetchOptions.FetchMode.EAGER;
import static org.infinispan.query.FetchOptions.FetchMode.LAZY;
import static org.infinispan.test.TestingUtil.withTx;
import static org.testng.AssertJUnit.*;

/**
 * @author Anna Manukyan
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.nulls.NullCollectionElementsClusteredTest")
public class NullCollectionElementsClusteredTest extends MultipleCacheManagersTest {

   private Cache cache1, cache2;
   private SearchManager searchManager;

   public void testQuerySkipsNullsInList() throws Exception {
      prepareData();
      withTx(tm(1), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache2.remove("2");   // cache will now be out of sync with the index
            searchManager = Search.getSearchManager(cache1);

            Query query = createQueryBuilder().keyword().onField("bar").matching("2").createQuery();
            List list = searchManager.getQuery(query).list();
            assertEquals("Wrong list size.", 0, list.size());
            return null;
         }
      });
   }

   public void testQuerySkipsNullsInEagerIterator() throws Exception {
      prepareData();

      withTx(tm(0), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache1.remove("1");   // cache will now be out of sync with the index
            searchManager = Search.getSearchManager(cache1);

            Query query = createQueryBuilder().keyword().onField("bar").matching("1").createQuery();
            ResultIterator iterator = searchManager.getQuery(query).iterator(new FetchOptions().fetchMode(EAGER));
            assertFalse("Iterator should not have elements.", iterator.hasNext());
            try {
               iterator.next();
               fail("Expected NoSuchElementException");
            } catch (NoSuchElementException e) {
               // pass
            }
            return null;
         }
      });
   }

   // This is the same as the verification above, only verifying the default iterator() method.
   public void testQuerySkipsNullsInDefaultIterator() throws Exception {
      prepareData();

      withTx(tm(0), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache1.remove("1");   // cache will now be out of sync with the index
            searchManager = Search.getSearchManager(cache1);

            Query query = createQueryBuilder().keyword().onField("bar").matching("1").createQuery();
            ResultIterator iterator = searchManager.getQuery(query).iterator();
            assertFalse(iterator.hasNext());
            try {
               iterator.next();
               fail("Expected NoSuchElementException");
            } catch (NoSuchElementException e) {
               // pass
            }
            return null;
         }
      });
   }

   // This is the same as the verification above, only verifying the iterating using cacheQuery object.
   public void testQuerySkipsNullsInCacheQueryIterator() throws Exception {
      prepareData();

      withTx(tm(0), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache1.remove("1");   // cache will now be out of sync with the index
            searchManager = Search.getSearchManager(cache1);

            Query query = createQueryBuilder().keyword().onField("bar").matching("1").createQuery();
            CacheQuery cacheQuery = searchManager.getQuery(query);
            //pruivo: the result size includes the null elements...
            assertEquals("Wrong result size.", 1, cacheQuery.getResultSize());

            for (Object obj : cacheQuery) {
               fail("The iterator should not contain any data but it contains " + obj);
            }

            return null;
         }
      });
   }

   public void testQuerySkipsNullsInLazyIterator() throws Exception {
      prepareData();

      withTx(tm(1), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache2.remove("2");   // cache will now be out of sync with the index
            searchManager = Search.getSearchManager(cache1);

            Query query = createQueryBuilder().keyword().onField("bar").matching("2").createQuery();
            ResultIterator iterator = searchManager.getQuery(query).iterator(new FetchOptions().fetchMode(LAZY));
            assertFalse(iterator.hasNext());
            try {
               iterator.next();
               fail("Expected NoSuchElementException");
            } catch (NoSuchElementException e) {
               // pass
            }
            return null;
         }
      });
   }

   public void testQueryReturnsNullWhenProjectingCacheValue() throws Exception {
      prepareData();

      withTx(tm(0), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache1.remove("1");   // cache will now be out of sync with the index
            searchManager = Search.getSearchManager(cache1);

            Query query = createQueryBuilder().keyword().onField("bar").matching("1").createQuery();
            ResultIterator iterator = searchManager.getQuery(query).projection(ProjectionConstants.VALUE, "bar")
                  .iterator(new FetchOptions().fetchMode(LAZY));
            assertTrue("Expected an element in iterator.", iterator.hasNext());
            Object[] projection = (Object[]) iterator.next();
            assertNull(projection[0]);
            assertEquals("1", projection[1]);
            return null;
         }
      });
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      cfg
            .indexing()
            .enable()
            .indexLocalOnly(true)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      createClusteredCaches(2, cfg);

      cache1 = cache(0);
      cache2 = cache(1);
   }

   private QueryBuilder createQueryBuilder() {
      return searchManager.buildQueryBuilderForClass(Foo.class).get();
   }

   private void prepareData() throws Exception {
      withTx(tm(0), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache1.put("1", new Foo("1"));
            return null;
         }
      });

      withTx(tm(1), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache2.put("2", new Foo("2"));
            return null;
         }
      });
   }

   @Indexed(index = "FooIndex")
   public static class Foo implements Serializable {
      private String bar;

      public Foo(String bar) {
         this.bar = bar;
      }

      @Field(name = "bar", store = Store.YES)
      public String getBar() {
         return bar;
      }
   }
}
