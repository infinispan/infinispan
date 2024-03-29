package org.infinispan.jcache.util;

import java.net.URI;
import java.util.Iterator;
import java.util.Properties;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;

/**
 * Testing utilities for JCache tests.
 *
 * @author Galder Zamarreño
 * @author Matej Cimbora
 * @since 5.3
 */
public class JCacheTestingUtil {

   private JCacheTestingUtil() {
      // Do not instantiate
   }

   /**
    * Run a task defined by the {@link Runnable} instance with a {@link CachingProvider} making sure that the caching
    * provider is closed after use.
    *
    * @param r task to execute with caching provider
    */
   public static void withCachingProvider(JCacheRunnable r) {
      ClassLoader tccl = Thread.currentThread().getContextClassLoader();
      CachingProvider p = Caching.getCachingProvider(new TestClassLoader(tccl));
      try {
         r.run(p);
      } finally {
         p.close();
      }
   }

   public static class TestClassLoader extends ClassLoader {
      public TestClassLoader(ClassLoader parent) {
         super(parent);
      }
   }

   public static void sleep(long duration) {
      try {
         Thread.sleep(duration);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new IllegalStateException(e);
      }
   }

   public static int getEntryCount(Iterator iterator) {
      int entryCount = 0;
      while (iterator.hasNext()) {
         iterator.next();
         entryCount++;
      }
      return entryCount;
   }

   public static Cache createCache(CacheManager manager, String cacheName) {
      return manager.createCache(cacheName, new MutableConfiguration());
   }

   public static CacheManager createCacheManager(CachingProvider provider, Properties properties,
                                                 String name, ClassLoader classLoader) {
      return provider.getCacheManager(URI.create("uri:" + name), classLoader, properties);
   }
}
