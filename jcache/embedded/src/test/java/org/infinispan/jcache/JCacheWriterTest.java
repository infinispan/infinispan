package org.infinispan.jcache;

import org.infinispan.jcache.embedded.JCacheManager;
import org.infinispan.jcache.util.InMemoryJCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.Int;
import org.testng.annotations.Test;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableConfiguration;
import java.lang.reflect.Method;
import java.net.URI;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

@Test(groups = "functional", testName = "jcache.JCacheWriterTest")
public class JCacheWriterTest extends AbstractInfinispanTest {

   public void testWriteCustomKey(Method m) {
      final String cacheName = m.getName();
      withCacheManager(new CacheManagerCallable(
         TestCacheManagerFactory.createCacheManager(false)) {
         @Override
         public void call() {
            JCacheManager jCacheManager = createJCacheManager(cm, this);
            InMemoryJCacheStore<Int, String> store = new InMemoryJCacheStore<>();

            MutableConfiguration<Int, String> cfg = new MutableConfiguration<Int, String>();
            cfg.setReadThrough(true);
            cfg.setCacheLoaderFactory(new FactoryBuilder.SingletonFactory(store));
            cfg.setCacheWriterFactory(new FactoryBuilder.SingletonFactory(store));
            Cache<Int, String> cache = jCacheManager.createCache(cacheName, cfg);

            cache.put(new Int(1), "v1");
            assertEquals("v1", store.load(new Int(1)));
            cache.put(new Int(2), "v2");
            assertEquals("v2", store.load(new Int(2)));

            assertEquals("v1", cache.get(new Int(1)));
            assertEquals("v2", cache.get(new Int(2)));

            cache.remove(new Int(1));
            assertNull(store.load(new Int(1)));
            cache.remove(new Int(2));
            assertNull(store.load(new Int(2)));
         }
      });
   }

   public void testWriteCustomValue(Method m) {
      final String cacheName = m.getName();
      withCacheManager(new CacheManagerCallable(
         TestCacheManagerFactory.createCacheManager(false)) {
         @Override
         public void call() {
            JCacheManager jCacheManager = createJCacheManager(cm, this);
            InMemoryJCacheStore<String, Int> store = new InMemoryJCacheStore<>();

            MutableConfiguration<String, Int> cfg = new MutableConfiguration<String, Int>();
            cfg.setReadThrough(true);
            cfg.setCacheLoaderFactory(new FactoryBuilder.SingletonFactory(store));
            cfg.setCacheWriterFactory(new FactoryBuilder.SingletonFactory(store));
            Cache<String, Int> cache = jCacheManager.createCache(cacheName, cfg);

            cache.put("k1", new Int(1));
            assertEquals(new Int(1), store.load("k1"));
            cache.put("k2", new Int(2));
            assertEquals(new Int(2), store.load("k2"));

            assertEquals(new Int(2), cache.get("k2"));
            assertEquals(new Int(1), cache.get("k1"));
         }
      });
   }


   private static JCacheManager createJCacheManager(EmbeddedCacheManager cm, Object creator) {
      return new JCacheManager(URI.create(creator.getClass().getName()), cm, null);
   }

}
