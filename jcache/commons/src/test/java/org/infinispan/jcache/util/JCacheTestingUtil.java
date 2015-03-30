package org.infinispan.jcache.util;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.Iterator;
import java.util.Properties;

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
         Thread.currentThread().sleep(duration);
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

   public static Cache createCacheWithProperties(CachingProvider provider, Class invoker, String cacheName, Properties properties) {
      CacheManager manager = provider.getCacheManager(URI.create(invoker.getName()), new TestClassLoader(Thread.currentThread().getContextClassLoader()), properties);
      return manager.createCache(cacheName, new MutableConfiguration());
   }

}
