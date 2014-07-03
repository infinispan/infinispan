package org.infinispan.query.blackbox;

import java.util.List;

import org.apache.lucene.search.Query;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.InterceptorConfiguration.Position;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.indexedembedded.Book;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests whether query caches can restart without problems.
 *
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @author Marko Luksa
 * @since 5.2
 */
@Test(testName = "query.blackbox.QueryCacheRestartTest", groups = "functional")
public class QueryCacheRestartTest extends AbstractInfinispanTest {

   public void testQueryCacheRestart() {
      queryCacheRestart(false);
   }

   public void testLocalQueryCacheRestart() {
      queryCacheRestart(true);
   }

   private void queryCacheRestart(boolean localOnly) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().index(localOnly ? Index.LOCAL : Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      final NoOpInterceptor noOpInterceptor = new NoOpInterceptor();
      builder.customInterceptors().addInterceptor().interceptor(noOpInterceptor).position(Position.FIRST);

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.getCache();
            addABook(cache);
            assertFindBook(cache);
            cache.stop();

            assertCacheHasCustomInterceptor(cache, noOpInterceptor);

            cache.start();
            //stopped cache lost all data, and in memory index is lost: re-store both
            //(not needed with permanent indexes and caches using a cachestore)
            addABook(cache);
            assertFindBook(cache);

            assertCacheHasCustomInterceptor(cache, noOpInterceptor);
         }

         protected void addABook(Cache<Object, Object> cache) {
            cache.put("1",
                  new Book("Infinispan Data Grid Platform",
                           "Francesco Marchioni and Manik Surtani",
                           "Packt Publishing"));
         }

      });
   }

   private void assertCacheHasCustomInterceptor(Cache<Object, Object> cache, CommandInterceptor interceptor) {
      for (InterceptorConfiguration interceptorConfig : cache.getCacheConfiguration().customInterceptors().interceptors()) {
         if (interceptor.equals(interceptorConfig.interceptor())) {
            return;
         }
      }

      fail("Expected to find interceptor " + interceptor + " among custom interceptors of cache, but it was not there.");
   }

   private static void assertFindBook(Cache<Object, Object> cache) {
      SearchManager searchManager = Search.getSearchManager(cache);
      QueryBuilder queryBuilder = searchManager.buildQueryBuilderForClass(Book.class).get();
      Query luceneQuery = queryBuilder.keyword().onField("title").matching("infinispan").createQuery();
      CacheQuery cacheQuery = searchManager.getQuery(luceneQuery);
      List<Object> list = cacheQuery.list();
      assertEquals(1, list.size());
   }

   private static class NoOpInterceptor extends CommandInterceptor {

   }
}
