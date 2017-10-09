package org.infinispan.jcache;

import static org.infinispan.jcache.util.JCacheTestingUtil.withCachingProvider;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.net.URI;

import javax.cache.Cache;
import javax.cache.configuration.MutableConfiguration;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jcache.embedded.JCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

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

   public void testJCacheManagerWherePathContainsFileSchemaAndAbsolutePath() throws Exception {
      URI uri = JCacheConfigurationTest.class.getClassLoader().getResource("infinispan_uri.xml").toURI();
      withCachingProvider(provider -> {
         JCacheManager jCacheManager = new JCacheManager(
               uri,
               provider.getClass().getClassLoader(),
               provider,
               null);
         assertTrue(null != jCacheManager.getCache("foo"));
      });
   }

   public void testJCacheManagerWherePathContainsJarFileSchema() throws Exception {
      URI uri = JCacheConfigurationTest.class.getClassLoader().getResource("infinispan_uri.xml").toURI();
      URI uriWithJarFileSchema = URI.create(uri.toString().replace("file", "jar:file"));

      withCachingProvider(provider -> {
         JCacheManager jCacheManager = new JCacheManager(
               uriWithJarFileSchema,
               provider.getClass().getClassLoader(),
               provider,
               null);
         assertTrue(null != jCacheManager.getCache("foo"));
      });
   }

   public void testJCacheManagerWithWildcardCacheConfigurations() throws Exception {
      URI uri = JCacheConfigurationTest.class.getClassLoader().getResource("infinispan_uri.xml").toURI();
      withCachingProvider(provider -> {
         JCacheManager jCacheManager = new JCacheManager(
               uri,
               provider.getClass().getClassLoader(),
               provider,
               null);
         Cache<Object, Object> wildcache1 = jCacheManager.createCache("wildcache1", new MutableConfiguration<>());
         org.infinispan.Cache unwrap = wildcache1.unwrap(org.infinispan.Cache.class);
         Configuration configuration = unwrap.getCacheConfiguration();
         assertEquals(10500, configuration.expiration().wakeUpInterval());
         assertEquals(11, configuration.expiration().lifespan());
         assertEquals(11, configuration.expiration().maxIdle());
      });
   }

}
