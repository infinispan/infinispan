package org.infinispan.jcache;

import org.infinispan.jcache.AbstractTwoCachesBasicOpsTest;
import org.infinispan.jcache.JCacheAnnotatedClass;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import javax.cache.Cache;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

import static org.infinispan.jcache.util.JCacheTestingUtil.TestClassLoader;
import static org.infinispan.jcache.util.JCacheTestingUtil.getCache;

/**
 * @author Matej Cimbora
 */
public class JCacheTwoCachesBasicOpsTest extends AbstractTwoCachesBasicOpsTest {

   private static CachingProvider cachingProvider;
   private static Cache cache1;
   private static Cache cache2;
   private static Cache expiryCache1;
   private static Cache expiryCache2;

   @BeforeClass
   public static void setUp() {
      cachingProvider = Caching.getCachingProvider(new TestClassLoader(Thread.currentThread().getContextClassLoader()));
      cache1 = getCache(cachingProvider, JCacheTwoCachesBasicOpsTest.class, "dist-basic-1.xml", "testCache");
      cache2 = getCache(cachingProvider, JCacheTwoCachesBasicOpsTest.class, "dist-basic-2.xml", "testCache");
      expiryCache1 = getCache(cachingProvider, JCacheTwoCachesBasicOpsTest.class, "dist-basic-1.xml", "expiryCache");
      expiryCache2 = getCache(cachingProvider, JCacheTwoCachesBasicOpsTest.class, "dist-basic-2.xml", "expiryCache");
   }

   @AfterClass
   public static void tearDown() {
      cache1.close();
      cache2.close();
      expiryCache1.close();
      expiryCache2.close();
      cachingProvider.close();
   }

   @After
   public void clearCaches() {
      cache1.clear();
      cache2.clear();
      expiryCache1.clear();
      expiryCache2.clear();
   }

   @Override
   public Cache getCache1() {
      return cache1;
   }

   @Override
   public Cache getCache2() {
      return cache2;
   }

   @Override
   public Cache getExpiryCache1() {
      return expiryCache1;
   }

   @Override
   public Cache getExpiryCache2() {
      return expiryCache2;
   }

}
