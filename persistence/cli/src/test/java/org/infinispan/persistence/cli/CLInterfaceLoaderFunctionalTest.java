package org.infinispan.persistence.cli;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.cli.configuration.CLInterfaceLoaderConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.MultiCacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.Key;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.infinispan.test.TestingUtil.withCacheManagers;
import static org.testng.AssertJUnit.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = "unit", testName = "persistence.cli.CLInterfaceLoaderFunctionalTest")
public class CLInterfaceLoaderFunctionalTest extends AbstractInfinispanTest {

   static final String SOURCE_CONNECTION_STRING = "jmx://localhost:2626/SourceCacheManager/___defaultcache";

   public void testSequentialGet() {
      withCacheManager(new CacheManagerCallable(createSourceCacheManager()) {
         @Override
         public void call() {
            Cache<Integer, String> sourceCache = cm.getCache();
            sourceCache.put(1, "v1");
            sourceCache.put(2, "v2");
            sourceCache.put(3, "v3");

            withCacheManager(new CacheManagerCallable(createTargetCacheManager()) {
               @Override
               public void call() {
                  Cache<Object, Object> targetCache = cm.getCache();
                  assertEquals("v1", targetCache.get(1));
                  assertEquals("v2", targetCache.get(2));
                  assertEquals("v3", targetCache.get(3));
               }
            });
         }
      });
   }

   public void testMultiThreadGet() {
      withCacheManager(new CacheManagerCallable(createSourceCacheManager()) {
         @Override
         public void call() {
            Cache<Integer, String> sourceCache = cm.getCache();
            sourceCache.put(1, "v1");
            sourceCache.put(2, "v2");
            sourceCache.put(3, "v3");

            withCacheManager(new CacheManagerCallable(createTargetCacheManager()) {
               @Override
               public void call() {
                  final Cache<Integer, String> targetCache = cm.getCache();
                  Future<String> f1 = asyncGet(targetCache, 1);
                  Future<String> f2 = asyncGet(targetCache, 2);
                  Future<String> f3 = asyncGet(targetCache, 3);

                  assertFutureEquals("v1", f1);
                  assertFutureEquals("v2", f2);
                  assertFutureEquals("v3", f3);
               }
            });
         }
      });
   }

   @Test(groups = "unstable") // Needs a way to convert keys to JSON format
   public void testCustomKey() {
      withCacheManager(new CacheManagerCallable(createSourceCacheManager()) {
         @Override
         public void call() {
            Cache<Key, Integer> sourceCache = cm.getCache();
            final Key k1 = new Key("k1", false);
            final Key k2 = new Key("k2", false);
            final Key k3 = new Key("k3", false);
            sourceCache.put(k1, 1);
            sourceCache.put(k2, 2);
            sourceCache.put(k3, 3);

            withCacheManager(new CacheManagerCallable(createTargetCacheManager()) {
               @Override
               public void call() {
                  Cache<Key, Integer> targetCache = cm.getCache();
                  assertEquals(new Integer(1), targetCache.get(k1));
                  assertEquals(new Integer(2), targetCache.get(k2));
                  assertEquals(new Integer(3), targetCache.get(k3));
               }
            });
         }
      });
   }

   public void testCustomValue() {
      withCacheManager(new CacheManagerCallable(createSourceCacheManager()) {
         @Override
         public void call() {
            Cache<Integer, Person> sourceCache = cm.getCache();
            final Person p1 = new Person("Jose Garza");
            final Person p2 = new Person("Willard Rogers");
            final Person p3 = new Person("B. Cunningham");
            sourceCache.put(1, p1);
            sourceCache.put(2, p2);
            sourceCache.put(3, p3);

            withCacheManager(new CacheManagerCallable(createTargetCacheManager()) {
               @Override
               public void call() {
                  Cache<Key, Integer> targetCache = cm.getCache();
                  assertEquals(p1, targetCache.get(1));
                  assertEquals(p2, targetCache.get(2));
                  assertEquals(p3, targetCache.get(3));
               }
            });
         }
      });
   }

   private Future<String> asyncGet(
         final Cache<Integer, String> targetCache, final Integer key) {
      return fork(new Callable<String>() {
         @Override
         public String call() throws Exception {
            return targetCache.get(key);
         }
      });
   }

   private void assertFutureEquals(String expected, Future<String> f) {
      try {
         assertEquals(expected, f.get());
      } catch (Exception e) {
         throw new AssertionError(e);
      }
   }

   private EmbeddedCacheManager createSourceCacheManager() {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.globalJmxStatistics().cacheManagerName("SourceCacheManager");
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.jmxStatistics().enable();
      return TestCacheManagerFactory.createCacheManager(global, builder);
   }

   private EmbeddedCacheManager createTargetCacheManager() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      // Configure target cache manager with the CLI cache loader pointing to source
      builder.persistence()
            .addStore(CLInterfaceLoaderConfigurationBuilder.class)
            .connectionString(SOURCE_CONNECTION_STRING);

      return TestCacheManagerFactory.createCacheManager(builder);
   }

}
