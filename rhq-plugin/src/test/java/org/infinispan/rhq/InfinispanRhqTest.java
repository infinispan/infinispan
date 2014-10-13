package org.infinispan.rhq;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import static org.infinispan.test.TestingUtil.*;

/**
 * Standalone cache for infinispan testing
 *
 * @author Heiko W. Rupp
 */
public class InfinispanRhqTest {

   private static final String MY_CUSTOM_CACHE = "myCustomCache";

   public static void main(String[] args) throws Exception {
      String indexDirectory = tmpDirectory(InfinispanRhqTest.class);
      try {
         run(indexDirectory);
      } finally {
         recursiveFileRemove(indexDirectory);
      }
   }

   private static void run(final String indexDirectory) {
      GlobalConfigurationBuilder globalConfigurationBuilder = new GlobalConfigurationBuilder();
      globalConfigurationBuilder.globalJmxStatistics().enable().jmxDomain("org.infinispan");

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(globalConfigurationBuilder, new ConfigurationBuilder())) {
         @Override
         public void call() {
            // org.infinispan:cache-name=myCustomcache(local),jmx-resource=CacheMgmgtInterceptor
            // org.infinispan:cache-name=myCustomcache(local),jmx-resource=MvccLockManager
            // org.infinispan:cache-name=myCustomcache(local),jmx-resource=TxInterceptor

            ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
            configurationBuilder.jmxStatistics().enable();
            configurationBuilder.eviction().maxEntries(123);
            configurationBuilder.expiration().maxIdle(180000);
            configurationBuilder.indexing()
                  .index(Index.ALL)
                  .addProperty("default.directory_provider", "filesystem")
                  .addProperty("default.indexBase", indexDirectory)
                  .addProperty("lucene_version", "LUCENE_CURRENT");

            cm.defineConfiguration(MY_CUSTOM_CACHE, configurationBuilder.build());

            Cache<String,String> cache = cm.getCache(MY_CUSTOM_CACHE);

            cache.put("myKey", "myValue");

            int i = 0;
            while (i < Integer.MAX_VALUE) {
               try {
                  Thread.sleep(12000);
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
               }
               cache.put("key" + i, String.valueOf(i));
               cache.get("key" + ((int)(10000 * Math.random())));
               i++;
               if (i%10 == 0) {
                  System.out.print(".");
                  System.out.flush();
               }
            }
         }
      });
   }
}
