package org.infinispan.jcache;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.URI;

import javax.cache.Cache;
import javax.cache.CacheManager;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jcache.embedded.JCache;
import org.infinispan.jcache.embedded.JCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "jcache.UnwrapTest")
public class UnwrapTest extends AbstractInfinispanTest {

   public void testUnwrap() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            cm.defineConfiguration("UnwrapCache", new ConfigurationBuilder().build());
            CacheManager jCacheManager = new JCacheManager(URI.create("UnwrapCacheManager"), cm, null);
            Cache<Object, Object> jcache = jCacheManager.getCache("UnwrapCache");
            assertNotNull(jCacheManager.unwrap(JCacheManager.class));
            assertNotNull(jcache.unwrap(JCache.class));
            assertNotNull(jCacheManager.unwrap(EmbeddedCacheManager.class));
            assertNotNull(jcache.unwrap(org.infinispan.Cache.class));
         }
      });
   }

}
