package org.infinispan.jcache;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jcache.embedded.JCache;
import org.infinispan.jcache.embedded.JCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.net.URI;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "functional", testName = "jcache.UnwrapTest")
public class UnwrapTest {

   public void testUnwrap() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(false)) {
         @Override
         public void call() {
            cm.defineConfiguration("UnwrapCache", new ConfigurationBuilder().build());
            CacheManager jCacheManager = new JCacheManager(URI.create("UnwrapCacheManager"), cm, null);
            Cache<Object, Object> jcache = jCacheManager.getCache("UnwrapCache");
            assertTrue(jCacheManager.unwrap(JCacheManager.class) != null);
            assertTrue(jcache.unwrap(JCache.class) != null);
            assertTrue(jCacheManager.unwrap(EmbeddedCacheManager.class) != null);
            assertTrue(jcache.unwrap(org.infinispan.Cache.class) != null);
         }
      });
   }

}
