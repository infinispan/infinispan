package org.infinispan.query.blackbox;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.indexedembedded.Book;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
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
   private EmbeddedCacheManager cm;

   public void testLocalIndexedCacheRestart() {
      indexedCacheRestart(false);
   }

   public void testLocalIndexedCacheRestartManager() {
      indexedCacheRestart(true);
   }

   private void indexedCacheRestart(boolean stopManager) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Book.class);

      cm = TestCacheManagerFactory.createCacheManager(builder);
      Cache<String, Book> cache = cm.getCache();
      assertNotNull(extractInterceptorChain(cache).findInterceptorExtending(QueryInterceptor.class));
      assertTrue(cache.isEmpty());
      addABook(cache);
      assertFindBook(cache);

      if (stopManager) {
         cm.stop();

         cm = TestCacheManagerFactory.createCacheManager(builder);
         cache = cm.getCache();
      } else {
         cache.stop();

         cache.start();
      }

      assertNotNull(extractInterceptorChain(cache).findInterceptorExtending(QueryInterceptor.class));
      assertTrue(cache.isEmpty());
      //stopped cache lost all data, and in memory index is lost: re-store both
      //(not needed with permanent indexes and caches using a cachestore)
      addABook(cache);
      assertFindBook(cache);
   }

   private void addABook(Cache<String, Book> cache) {
      cache.put("1",
            new Book("Infinispan Data Grid Platform",
                  "Francesco Marchioni and Manik Surtani",
                  "Packt Publishing"));
   }

   private static void assertFindBook(Cache<?, ?> cache) {
      String q = String.format("FROM %s WHERE title:'infinispan'", Book.class.getName());
      Query cacheQuery = cache.query(q);
      List<Book> list = cacheQuery.list();
      assertEquals(1, list.size());
   }

   @AfterMethod(alwaysRun = true)
   public void stopManager() {
      cm.stop();
   }

   static class NoOpInterceptor extends DDAsyncInterceptor {

   }
}
