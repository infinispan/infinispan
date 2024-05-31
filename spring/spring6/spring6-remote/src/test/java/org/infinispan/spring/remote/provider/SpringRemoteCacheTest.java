package org.infinispan.spring.remote.provider;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.cache.Cache;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

@Test(testName = "spring.provider.SpringRemoteCacheTest", groups = "functional")
public class SpringRemoteCacheTest extends SingleCacheManagerTest {

   private static final String TEST_CACHE_NAME = "SerializationCache";
   private static final String TEST_CACHE_NAME_PROTO = "ProtoStreamCache";

   private RemoteCacheManager remoteCacheManager;
   private HotRodServer hotrodServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cacheManager.defineConfiguration(TEST_CACHE_NAME,
                                       hotRodCacheConfiguration(MediaType.APPLICATION_SERIALIZED_OBJECT).build());
      cacheManager.defineConfiguration(TEST_CACHE_NAME_PROTO,
                                       hotRodCacheConfiguration(MediaType.APPLICATION_PROTOSTREAM).build());
      cache = cacheManager.getCache(TEST_CACHE_NAME);

      return cacheManager;
   }

   @BeforeClass
   public void setupRemoteCacheFactory() {
      hotrodServer = HotRodTestingUtil.startHotRodServer(cacheManager, 0);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host("localhost").port(hotrodServer.getPort());
      builder.remoteCache(TEST_CACHE_NAME).marshaller(JavaSerializationMarshaller.class);
      builder.remoteCache(TEST_CACHE_NAME_PROTO).marshaller(ProtoStreamMarshaller.class);
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

      CyclicBarrier barrier = new CyclicBarrier(2);
      Future<String> thread1 = fork(() -> cache.get("test", () -> {
         barrier.await(10, TimeUnit.SECONDS);
         barrier.await(10, TimeUnit.SECONDS);
         return "thread1";
      }));

      // Ensure first retrieval is in lambda before continuing
      barrier.await(10, TimeUnit.SECONDS);

      Future<String> thread2 = fork(() ->  cache.get("test", () -> "thread2"));

      barrier.await(1, TimeUnit.SECONDS);

      String valueObtainedByThread1 = thread1.get();
      String valueObtainedByThread2 = thread2.get();

      Cache.ValueWrapper valueAfterGetterIsDone = cache.get("test");

      //then
      assertNotNull(valueAfterGetterIsDone);
      assertEquals("thread1", valueAfterGetterIsDone.get());
      assertEquals("thread1", valueObtainedByThread1);
      assertEquals("thread1", valueObtainedByThread2);
   }

   @Test(timeOut = 30_000)
   public void testRetrieveMethods() throws Exception {
      final SpringRemoteCacheManager springRemoteCacheManager = new SpringRemoteCacheManager(remoteCacheManager, true);
      final SpringCache cache = springRemoteCacheManager.getCache(TEST_CACHE_NAME);

      cache.put("test-one", "one");
      String result = (String) ((Cache.ValueWrapper) cache.retrieve("test-one").get(1, TimeUnit.SECONDS)).get();
      assertEquals("one", result);

      result = cache.retrieve("test-two", () -> CompletableFuture.completedFuture("two")).get();
      assertEquals("two", result);

      CyclicBarrier barrier = new CyclicBarrier(2);

      Future<String> thread1 = fork(() -> cache.retrieve("test-3", () -> {
         try {
            barrier.await(10, TimeUnit.SECONDS);
            barrier.await(10, TimeUnit.SECONDS);
         } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
         }
         return CompletableFuture.completedFuture("thread1");
      }).get());

      // Ensure first retrieval is in lambda before continuing
      barrier.await(10, TimeUnit.SECONDS);

      Future<String> thread2 = fork(() -> cache.retrieve("test-3",
            () -> CompletableFuture.completedFuture("thread2")
      ).get());

      barrier.await(1, TimeUnit.SECONDS);

      String valueObtainedByThread1 = thread1.get();
      String valueObtainedByThread2 = thread2.get();

      Cache.ValueWrapper valueAfterGetterIsDoneTest2 = cache.get("test-3");
      assertNotNull(valueAfterGetterIsDoneTest2);
      assertEquals("thread1", valueAfterGetterIsDoneTest2.get());
      assertEquals("thread1", valueObtainedByThread1);
      assertEquals("thread1", valueObtainedByThread2);
   }

   @Test(timeOut = 30_000)
   public void testRetrieveMethodsWithNoReactive() {
      final SpringRemoteCacheManager springRemoteCacheManager = new SpringRemoteCacheManager(remoteCacheManager, false);
      final SpringCache cache = springRemoteCacheManager.getCache(TEST_CACHE_NAME);

      assertThrowsExactly(UnsupportedOperationException.class, () -> cache.retrieve("test-3").get(10, TimeUnit.SECONDS));
      assertThrowsExactly(UnsupportedOperationException.class, () -> cache.retrieve("test-3", () -> CompletableFuture.completedFuture("hello"))
              .get(10, TimeUnit.SECONDS));
   }

   @DataProvider(name = "caches")
   public Object[][] caches() {
      return new Object[][] {
            {TEST_CACHE_NAME},
            {TEST_CACHE_NAME_PROTO},
      };
   }

   @Test(dataProvider = "caches")
   public void testNullValues(String cacheName) {
      //given
      final SpringRemoteCacheManager springRemoteCacheManager = new SpringRemoteCacheManager(remoteCacheManager);
      final SpringCache cache = springRemoteCacheManager.getCache(cacheName);

      // when
      cache.put("key", null);

      // then
      Cache.ValueWrapper valueWrapper = cache.get("key");
      assertNotNull(valueWrapper);
      assertNull(valueWrapper.get());

      if (cacheName.equals(TEST_CACHE_NAME_PROTO)) {
         // The server should be able to convert application/x-protostream to application/json
         RemoteCache<?, ?> nativeCache = (RemoteCache<?, ?>) cache.getNativeCache();
         RemoteCache<Object, Object> jsonCache =
               nativeCache.withDataFormat(DataFormat.builder()
                                                    .valueType(MediaType.APPLICATION_JSON)
                                                    .valueMarshaller(new UTF8StringMarshaller())
                                                    .build());
         Object jsonValue = jsonCache.get("key");
         assertEquals("{\"_type\":\"org.infinispan.commons.NullValue\"}", jsonValue);
      }
   }
}
