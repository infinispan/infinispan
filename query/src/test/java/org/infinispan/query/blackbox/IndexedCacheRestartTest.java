package org.infinispan.query.blackbox;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.InterceptorConfiguration.Position;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.indexedembedded.Book;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests whether indexed caches can restart without problems.
 *
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @author Marko Luksa
 * @since 5.2
 */
@Test(testName = "query.blackbox.IndexedCacheRestartTest", groups = "functional")
public class IndexedCacheRestartTest extends AbstractInfinispanTest {

   public void testIndexedCacheRestart() {
      indexedCacheRestart(false);
   }

   public void testLocalIndexedCacheRestart() {
      indexedCacheRestart(true);
   }

   private void indexedCacheRestart(boolean localOnly) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
            .addIndexedEntity(Book.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      // Test that the query module doesn't break user-configured custom interceptors
      // Not needed since ISPN-10262, as the query module is no longer configuring any custom interceptors
      final NoOpInterceptor noOpInterceptor = new NoOpInterceptor();
      builder.customInterceptors().addInterceptor().interceptor(noOpInterceptor).position(Position.FIRST);

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            Cache<String, Book> cache = cm.getCache();
            assertNotNull(extractInterceptorChain(cache).findInterceptorExtending(QueryInterceptor.class));
            assertTrue(cache.isEmpty());
            addABook(cache);
            assertFindBook(cache);
            cache.stop();

            assertCacheHasCustomInterceptor(cache, noOpInterceptor);

            cache.start();
            assertNotNull(extractInterceptorChain(cache).findInterceptorExtending(QueryInterceptor.class));
            assertTrue(cache.isEmpty());
            //stopped cache lost all data, and in memory index is lost: re-store both
            //(not needed with permanent indexes and caches using a cachestore)
            addABook(cache);
            assertFindBook(cache);

            assertCacheHasCustomInterceptor(cache, noOpInterceptor);
         }

         private void addABook(Cache<String, Book> cache) {
            cache.put("1",
                  new Book("Infinispan Data Grid Platform",
                        "Francesco Marchioni and Manik Surtani",
                        "Packt Publishing"));
         }
      });
   }

   private void assertCacheHasCustomInterceptor(Cache<?, ?> cache, AsyncInterceptor interceptor) {
      for (InterceptorConfiguration interceptorConfig : cache.getCacheConfiguration().customInterceptors().interceptors()) {
         if (interceptor == interceptorConfig.asyncInterceptor()) {
            return;
         }
      }

      fail("Expected to find interceptor " + interceptor + " among custom interceptors of cache, but it was not there.");
   }

   private static void assertFindBook(Cache<?, ?> cache) {
      SearchManager searchManager = Search.getSearchManager(cache);
      String q = String.format("FROM %s WHERE title:'infinispan'", Book.class.getName());
      CacheQuery<Book> cacheQuery = searchManager.getQuery(q, IndexedQueryMode.FETCH);
      List<Book> list = cacheQuery.list();
      assertEquals(1, list.size());
   }

   static class NoOpInterceptor extends DDAsyncInterceptor {

   }
}
