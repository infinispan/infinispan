package org.infinispan.jcache;

import static org.infinispan.jcache.util.JCacheTestingUtil.withCachingProvider;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.net.URI;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jcache.embedded.JCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = "functional", testName = "jcache.JCacheConfigurationTest")
public class JCacheConfigurationTest extends AbstractInfinispanTest {

   public void testNamedCacheConfiguration() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(false)) {
         @Override
         public void call() {
            cm.defineConfiguration("oneCache", new ConfigurationBuilder().build());
            JCacheManager jCacheManager = new JCacheManager(URI.create("oneCacheManager"), cm, null);
            assertTrue(null != jCacheManager.getCache("oneCache"));
         }
      });
   }

   public void testJCacheManagerWherePathContainsFileSchema() throws IOException {
      withCachingProvider(provider -> new JCacheManager(
            URI.create("file:infinispan_uri.xml"),
            provider.getClass().getClassLoader(),
            provider,
            null));
   }

   public void testJCacheManagerWherePathContainsJarFileSchema() throws IOException {
      withCachingProvider(provider -> new JCacheManager(
            URI.create("jar:file:infinispan_uri.xml"),
            provider.getClass().getClassLoader(),
            provider,
            null));
   }

}
