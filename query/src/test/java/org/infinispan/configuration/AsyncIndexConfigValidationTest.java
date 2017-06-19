package org.infinispan.configuration;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.Assert.assertTrue;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.hibernate.search.spi.InfinispanIntegration;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.indexmanager.InfinispanIndexManager;
import org.infinispan.query.test.Person;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @since 9.1
 */
@Test(groups = "functional", testName = "configuration.AsyncIndexConfigValidationTest")
public class AsyncIndexConfigValidationTest extends AbstractInfinispanTest {

   public void testWithDefaultIndexCaches() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createClusteredCacheManager()) {
         @Override
         public void call() throws Exception {
            Configuration indexConfig = createAsyncIndexConfig();
            cm.defineConfiguration(InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME, indexConfig);
            cm.defineConfiguration(InfinispanIntegration.DEFAULT_LOCKING_CACHENAME, indexConfig);
            cm.defineConfiguration(InfinispanIntegration.DEFAULT_INDEXESMETADATA_CACHENAME, indexConfig);
            cm.defineConfiguration("userCache", createUserCacheConfig());

            Cache<String, Person> userCache = cm.getCache("userCache");
            assertValidationExceptionThrown(() -> userCache.put("1", new Person()));
         }
      });
   }

   public void testWithCustomIndexCaches() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createClusteredCacheManager()) {
         @Override
         public void call() throws Exception {
            Configuration indexConfig = createAsyncIndexConfig();
            cm.defineConfiguration("data", indexConfig);
            cm.defineConfiguration("lock", indexConfig);
            cm.defineConfiguration("metadata", indexConfig);
            cm.defineConfiguration("userCache", createUserCacheConfigWithCustomIndexCaches());

            Cache<String, Person> userCache = cm.getCache("userCache");
            assertValidationExceptionThrown(() -> userCache.put("1", new Person()));
         }
      });
   }

   private Throwable getRootCauseException(Throwable re) {
      Throwable cause = re.getCause();
      if (cause instanceof RuntimeException)
         return getRootCauseException(cause);
      else
         return re;
   }

   private void assertValidationExceptionThrown(Runnable operation) {
      try {
         operation.run();
      } catch (RuntimeException ex) {
         Throwable cause = getRootCauseException(ex);
         assertTrue(cause instanceof IllegalArgumentException);
         String message = cause.getMessage();
         assertTrue(message.matches("ISPN(\\d)*: Lucene Directory for index 'person' cannot use cache '(.*)' with " +
               "mode 'REPL_ASYNC'. Only SYNC caches are supported!"));
      }
   }

   private Configuration createAsyncIndexConfig() {
      ConfigurationBuilder indexCfgBuilder = new ConfigurationBuilder();
      indexCfgBuilder.indexing().index(Index.NONE);
      indexCfgBuilder.clustering().cacheMode(CacheMode.REPL_ASYNC);
      return indexCfgBuilder.build();
   }

   private Configuration createUserCacheConfig() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing()
            .index(Index.LOCAL)
            .addProperty("default.indexmanager", InfinispanIndexManager.class.getName())
            .addProperty("error_handler", StaticTestingErrorHandler.class.getName());
      return builder.build();
   }

   private Configuration createUserCacheConfigWithCustomIndexCaches() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing()
            .index(Index.LOCAL)
            .addProperty("default.indexmanager", InfinispanIndexManager.class.getName())
            .addProperty("default.locking_cachename", "lock")
            .addProperty("default.data_cachename", "data")
            .addProperty("default.metadata_cachename", "metadata")
            .addProperty("error_handler", StaticTestingErrorHandler.class.getName());
      return builder.build();
   }

}
