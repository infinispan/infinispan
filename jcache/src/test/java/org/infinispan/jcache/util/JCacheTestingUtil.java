package org.infinispan.jcache.util;

import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

/**
 * Testing utilities for JCache tests.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class JCacheTestingUtil {

   private JCacheTestingUtil() {
      // Do not instantiate
   }

   /**
    * Run a task defined by the {@link Runnable} instance with a
    * {@link CachingProvider} making sure that the caching provider is
    * closed after use.
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

   private static class TestClassLoader extends ClassLoader {
      public TestClassLoader(ClassLoader parent) {
         super(parent);
      }
   }

}
