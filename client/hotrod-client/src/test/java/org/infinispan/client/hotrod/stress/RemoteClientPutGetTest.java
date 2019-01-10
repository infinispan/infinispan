package org.infinispan.client.hotrod.stress;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Simple PUTs/GETs test to verify performance regressions. Requires an external running server.
 */
@Test(groups = "stress", testName = "org.infinispan.client.hotrod.stress.RemoteClientPutGetTest", timeOut = 15*60*1000)
public class RemoteClientPutGetTest {

   private RemoteCache<String, Object> cache;

   private static final int NUMBER_OF_ENTRIES = 100000;
   private static final int THREAD_COUNT = 10;
   private static final int GET_OPERATIONS = 1000000;

   public static void main(String[] args) throws Exception {
      RemoteClientPutGetTest testCase = new RemoteClientPutGetTest();
      testCase.prepare();
      testCase.putTest();
      testCase.getTest();
   }

   @BeforeClass
   public void prepare() {
      RemoteCacheManager cacheManager = new RemoteCacheManager(
         HotRodClientTestingUtil.newRemoteConfigurationBuilder().addServer().host("localhost").port(11222).build());
      cache = cacheManager.getCache();
      cache.clear();
   }

   public void putTest() throws Exception {
      Thread[] threads = new Thread[THREAD_COUNT];
      for (int i = 0; i < THREAD_COUNT; i++) {
         final int thread_index = i;
         threads[i] = new Thread(() -> {
            for (int j = 0; j < NUMBER_OF_ENTRIES; j++) {
               cache.put("key_" + thread_index + "_" + j, UUID.randomUUID().toString());
            }
         });
      }
      Long start = System.nanoTime();
      for (int i = 0; i < THREAD_COUNT; i++) {
         threads[i].start();
      }
      for (int i = 0; i < THREAD_COUNT; i++) {
         threads[i].join();
      }
      Long elapsed = System.nanoTime() - start;
      System.out.format("Puts took: %,d s", TimeUnit.NANOSECONDS.toSeconds(elapsed));
   }

   public void getTest() throws Exception {
      Thread[] threads = new Thread[THREAD_COUNT];
      for (int i = 0; i < THREAD_COUNT; i++) {
         final int thread_index = i;
         threads[i] = new Thread(() -> {
            Random r = new Random(thread_index);
            for (int j = 0; j < GET_OPERATIONS; j++) {
               int key_id = r.nextInt(NUMBER_OF_ENTRIES);
               cache.get("key_" + thread_index + "_" + key_id);
            }
         });
      }
      Long start = System.nanoTime();
      for (int i = 0; i < THREAD_COUNT; i++) {
         threads[i].start();
      }
      for (int i = 0; i < THREAD_COUNT; i++) {
         threads[i].join();
      }
      Long elapsed = System.nanoTime() - start;
      System.out.format("\nGets took: %,d s", TimeUnit.NANOSECONDS.toSeconds(elapsed));
   }
}
