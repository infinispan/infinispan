package org.infinispan.client.hotrod.stress;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.test.fwk.TestResourceTracker;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Simple PUTs/GETs test to verify performance regressions. Requires an external running server.
 */
@Test(groups = "stress", testName = "org.infinispan.client.hotrod.stress.RemoteClientPutGetTest", timeOut = 15*60*1000)
public class RemoteClientPutGetTest {

   private RemoteCache<String, Object> cache;

   private static final int NUMBER_OF_ENTRIES = 100_000;
   private static final int THREAD_COUNT = 6;
   private static final int GET_OPERATIONS = 100_000;

   public static void main(String[] args) throws Exception {
      RemoteClientPutGetTest testCase = new RemoteClientPutGetTest();
      testCase.prepare();
      testCase.putTest();
      testCase.getTest();
   }

   @BeforeClass
   public void prepare() {
      TestResourceTracker.testStarted(RemoteClientPutGetTest.class.getName());
      RemoteCacheManager cacheManager = new RemoteCacheManager(
         HotRodClientTestingUtil.newRemoteConfigurationBuilder().addServer().host("localhost").port(11222)
                                .version(ProtocolVersion.PROTOCOL_VERSION_21)
                                .build());
      cache = cacheManager.getCache();
      cache.clear();
   }

   @AfterClass(alwaysRun = true)
   public void afterClass() throws IOException {
      cache.getRemoteCacheManager().close();
   }
   public void putTest() throws Exception {
      Thread[] threads = new Thread[THREAD_COUNT];
      for (int i = 0; i < THREAD_COUNT; i++) {
         final int thread_index = i;
         threads[i] = new Thread(() -> {
            for (int j = 0; j < NUMBER_OF_ENTRIES; j++) {
               cache.put("key_" + thread_index + "_" + j, UUID.randomUUID().toString());
               if (j % 2 == 0) {
                  cache.remove("key_" + thread_index + "_" + (j / 2));
               }
            }
         });
      }
      long start = System.nanoTime();
      for (int i = 0; i < THREAD_COUNT; i++) {
         threads[i].start();
      }
      for (int i = 0; i < THREAD_COUNT; i++) {
         threads[i].join();
      }
      long elapsed = System.nanoTime() - start;
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
      long start = System.nanoTime();
      for (int i = 0; i < THREAD_COUNT; i++) {
         threads[i].start();
      }
      for (int i = 0; i < THREAD_COUNT; i++) {
         threads[i].join();
      }
      long elapsed = System.nanoTime() - start;
      System.out.format("\nGets took: %,d s", TimeUnit.NANOSECONDS.toSeconds(elapsed));
   }
}
