package org.infinispan.spring.remote.provider;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.cache.Cache;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(testName = "spring.provider.SpringRemoteCacheTest", groups = "functional")
public class SpringRemoteCacheTest extends SingleCacheManagerTest {

   private static final String TEST_CACHE_NAME = "spring.remote.cache.Test";

   private RemoteCacheManager remoteCacheManager;
   private HotRodServer hotrodServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cacheManager.defineConfiguration(TEST_CACHE_NAME, cacheManager.getDefaultCacheConfiguration());
      cache = cacheManager.getCache(TEST_CACHE_NAME);

      return cacheManager;
   }

   @BeforeClass
   public void setupRemoteCacheFactory() {
      hotrodServer = HotRodTestingUtil.startHotRodServer(cacheManager, 0);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host("localhost").port(hotrodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(builder.build());
   }

   @AfterClass
   public void destroyRemoteCacheFactory() {
      remoteCacheManager.stop();
      hotrodServer.stop();
   }

   /*
    * In this test Thread 1 should exclusively block Cache#get method so that Thread 2 won't be able to
    * insert "thread2" string into the cache.
    *
    * The test check this part of the Spring spec:
    * Return the value to which this cache maps the specified key, obtaining that value from valueLoader if necessary.
    * This method provides a simple substitute for the conventional "if cached, return; otherwise create, cache and return" pattern.
    * @see http://docs.spring.io/spring/docs/current/javadoc-api/org/springframework/cache/Cache.html#get-java.lang.Object-java.util.concurrent.Callable-
    */
   @Test(timeOut = 30_000)
   public void testValueLoaderWithLocking() throws Exception {
      //given
      final SpringRemoteCacheManager springRemoteCacheManager = new SpringRemoteCacheManager(remoteCacheManager);
      final SpringCache cache = springRemoteCacheManager.getCache(TEST_CACHE_NAME);

      CountDownLatch waitUntilThread1LocksValueGetter = new CountDownLatch(1);

      //when
      Future<String> thread1 = fork(() -> cache.get("test", () -> {
         waitUntilThread1LocksValueGetter.countDown();
//         /TimeUnit.MILLISECONDS.sleep(10);
         return "thread1";
      }));

      Future<String> thread2 = fork(() -> {
         waitUntilThread1LocksValueGetter.await();
         return cache.get("test", () -> "thread2");
      });

      String valueObtainedByThread1 = thread1.get();
      String valueObtainedByThread2 = thread2.get();

      Cache.ValueWrapper valueAfterGetterIsDone = cache.get("test");

      //then
      assertNotNull(valueAfterGetterIsDone);
      assertEquals("thread1", valueAfterGetterIsDone.get());
      assertEquals("thread1", valueObtainedByThread1);
      assertEquals("thread1", valueObtainedByThread2);
   }
}
