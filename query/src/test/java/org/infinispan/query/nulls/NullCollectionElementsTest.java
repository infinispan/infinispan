package org.infinispan.query.nulls;

import org.apache.lucene.search.Query;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ProjectionConstants;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

import static org.infinispan.query.FetchOptions.FetchMode.*;
import static org.infinispan.test.TestingUtil.withTx;
import static org.junit.Assert.*;

/**
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 */
@Test(groups = "functional", testName = "query.nulls.NullCollectionElementsTest")
public class NullCollectionElementsTest extends SingleCacheManagerTest {

   private SearchManager searchManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .indexing()
             .enable()
             .indexLocalOnly(true)
             .addProperty("default.directory_provider", "ram")
             .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      searchManager = Search.getSearchManager(cache);
   }

   @BeforeMethod
   public void insertData() throws Exception {
      withTx(tm(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.put("1", new Foo("1"));
            return null;
         }
      });
   }

   @Test
   public void testQuerySkipsNullsInList() throws Exception {
      withTx(tm(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.remove("1");   // cache will now be out of sync with the index
            Query query = createQueryBuilder().keyword().onField("bar").matching("1").createQuery();
            List list = searchManager.getQuery(query).list();
            assert list.size() == 0;
            return null;
         }
      });
   }

   @Test
   public void testQuerySkipsNullsInEagerIterator() throws Exception {
      withTx(tm(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.remove("1");   // cache will now be out of sync with the index
            Query query = createQueryBuilder().keyword().onField("bar").matching("1").createQuery();
            ResultIterator iterator = searchManager.getQuery(query).iterator(new FetchOptions().fetchMode(EAGER));
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

   @Test // This is the same as the verification above, only verifying the default iterator() method.
   public void testQuerySkipsNullsInDefaultIterator() throws Exception {
      withTx(tm(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.remove("1");   // cache will now be out of sync with the index
            Query query = createQueryBuilder().keyword().onField("bar").matching("1").createQuery();
            CacheQuery cacheQuery = searchManager.getQuery(query);
            assertEquals(1, cacheQuery.getResultSize());
            ResultIterator iterator = cacheQuery.iterator();
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

   @Test
   public void testQuerySkipsNullsInLazyIterator() throws Exception {
      withTx(tm(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.remove("1");   // cache will now be out of sync with the index
            Query query = createQueryBuilder().keyword().onField("bar").matching("1").createQuery();
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

   @Test
   public void testQueryReturnsNullWhenProjectingCacheValue() throws Exception {
      withTx(tm(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.remove("1");   // cache will now be out of sync with the index
            Query query = createQueryBuilder().keyword().onField("bar").matching("1").createQuery();
            ResultIterator iterator = searchManager.getQuery(query).projection(ProjectionConstants.VALUE, "bar").iterator(new FetchOptions().fetchMode(LAZY));
            assertTrue(iterator.hasNext());
            Object[] projection = (Object[]) iterator.next();
            assertNull(projection[0]);
            assertEquals("1", projection[1]);
            return null;
         }
      });
   }

   private QueryBuilder createQueryBuilder() {
      return searchManager.buildQueryBuilderForClass(Foo.class).get();
   }


   @Indexed(index = "FooIndex")
   public class Foo {
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
