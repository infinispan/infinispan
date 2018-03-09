package org.infinispan.commands;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.infinispan.test.TestingUtil.wrapInboundInvocationHandler;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.inboundhandler.AbstractDelegatingHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "commands.ForgetInvocationTest")
public class ForgetInvocationsTest extends MultipleCacheManagersTest {
   private InvocationManager invocationManager0;
   private DataContainer dataContainer0;
   private DataContainer dataContainer1;
   private int maxBufferedPerSegment;
   private List<Runnable> cleanup = new ArrayList<>();

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.expiration().reaperEnabled(false);
      createCluster(builder, 3);
      InvocationManager im0 = extractComponent(cache(0), InvocationManager.class);
      invocationManager0 = Mockito.spy(im0);
      replaceComponent(cache(0), InvocationManager.class, invocationManager0, true);
      dataContainer0 = extractComponent(cache(0), DataContainer.class);
      dataContainer1 = extractComponent(cache(1), DataContainer.class);
      maxBufferedPerSegment = TestingUtil.extractField(invocationManager0, "MAX");
      cache(2); // make sure it's initialized
   }

   @AfterMethod(alwaysRun = true)
   protected void cleanup() throws Throwable {
      cleanup.forEach(Runnable::run);
   }

   public void testPutSingleKeyOnPrimary() throws Exception {
      testPutSingleKey(true, cache(0), cache(1));
   }

   public void testPutSingleKeyOnBackup() throws Exception {
      testPutSingleKey(false, cache(1), cache(0));
   }

   private void testPutSingleKey(boolean isPrimary, Cache primary, Cache backup) throws InterruptedException, BrokenBarrierException, TimeoutException {
      MagicKey key = new MagicKey(primary, backup);
      // we insert entries from non-primary owner
      for (int i = 0; i < maxBufferedPerSegment; ++i) {
         cache(0).put(key, "v" + i);
      }
      checkDataContainer(dataContainer0, key, maxBufferedPerSegment > 0, isPrimary ? 0 : maxBufferedPerSegment);
      checkDataContainer(dataContainer1, key, maxBufferedPerSegment > 0, maxBufferedPerSegment);

      CyclicBarrier barrier = new CyclicBarrier(2);
      wrapInboundInvocationHandler(cache(1), current -> {
         cleanup.add(() -> wrapInboundInvocationHandler(cache(1), ignored -> current));
         return new BarrierHandler(current, barrier);
      });

      CountDownLatch latch = new CountDownLatch(1);
      doAnswer(invocation -> {
         try {
            return invocation.callRealMethod();
         } finally {
            latch.countDown();
         }
      }).when(invocationManager0).notifyCompleted(any(), any(), anyInt());

      cache(0).put(key, "x");

      // Notifications are executed in the thread that receives confirmation from backup
      // but the main invoking thread can be released before such follow-up finishes
      assertTrue(latch.await(10000, TimeUnit.SECONDS));
      checkDataContainer(dataContainer0, key, true, 0);
      checkDataContainer(dataContainer1, key, true, maxBufferedPerSegment + 1);

      barrier.await(10, TimeUnit.SECONDS);
      barrier.await(10, TimeUnit.SECONDS);
      checkDataContainer(dataContainer1, key, true, 0);
   }

   public void testPutTwoKeysOnPrimary() throws Exception {
      testPutTwoKeys(true, cache(0), cache(1));
   }

   public void testPutTwoKeysOnBackup() throws Exception {
      testPutTwoKeys(false, cache(1), cache(0));
   }

   private void testPutTwoKeys(boolean isPrimary, Cache primary, Cache backup) throws InterruptedException, BrokenBarrierException, TimeoutException {
      MagicKey key1 = new MagicKey("first", primary, backup);
      MagicKey key2 = new MagicKey("second", key1);
      for (int i = 0; i < maxBufferedPerSegment; ++i) {
         cache(0).put(i % 2 == 0 ? key1 : key2, "v" + i);
      }
      int remnant = maxBufferedPerSegment % 2;
      checkDataContainer(dataContainer0, key1, maxBufferedPerSegment > 0, isPrimary ? 0 : maxBufferedPerSegment / 2 + remnant);
      checkDataContainer(dataContainer1, key2, maxBufferedPerSegment > 1, maxBufferedPerSegment / 2);

      CyclicBarrier barrier = new CyclicBarrier(2);
      wrapInboundInvocationHandler(cache(1), current -> {
         cleanup.add(() -> wrapInboundInvocationHandler(cache(1), ignored -> current));
         return new BarrierHandler(current, barrier);
      });

      CountDownLatch latch = new CountDownLatch(1);
      doAnswer(invocation -> {
         try {
            return invocation.callRealMethod();
         } finally {
            latch.countDown();
         }
      }).when(invocationManager0).notifyCompleted(any(), any(), anyInt());

      cache(0).put(key2, "x");

      // Notifications are executed in the thread that receives confirmation from backup
      // but the main invoking thread can be released before such follow-up finishes
      assertTrue(latch.await(10, TimeUnit.SECONDS));
      checkDataContainer(dataContainer0, key1, maxBufferedPerSegment > 0, 0);
      checkDataContainer(dataContainer0, key2, true, 0);
      checkDataContainer(dataContainer1, key1, maxBufferedPerSegment > 0, maxBufferedPerSegment/2 + remnant);
      checkDataContainer(dataContainer1, key2, true, maxBufferedPerSegment/2 + 1);

      barrier.await(10, TimeUnit.SECONDS);
      barrier.await(10, TimeUnit.SECONDS);
      checkDataContainer(dataContainer1, key1, maxBufferedPerSegment > 0, 0);
      checkDataContainer(dataContainer1, key2, true, 0);
   }

   public void testPutMapOnPrimary() throws Exception {
      testPutMap(true, cache(0), cache(1));
   }

   public void testPutMapOnBackup() throws Exception {
      testPutMap(false, cache(1), cache(0));
   }

   private void testPutMap(boolean isPrimary, Cache primary, Cache backup) throws Exception {
      MagicKey key1 = new MagicKey("first", primary, backup);
      MagicKey key2 = new MagicKey("second", key1);
      for (int i = 0; i < maxBufferedPerSegment; i += 2) {
         Map<Object, String> map = new HashMap<>();
         map.put(key1, "v" + i);
         if (i < maxBufferedPerSegment - 1) {
            map.put(key2, "v" + i);
         }
         cache(0).putAll(map);
      }
      int remnant = maxBufferedPerSegment % 2;
      checkDataContainer(dataContainer0, key1, maxBufferedPerSegment > 0, isPrimary ? 0 : maxBufferedPerSegment / 2 + remnant);
      checkDataContainer(dataContainer1, key2, maxBufferedPerSegment > 1, maxBufferedPerSegment / 2);

      CyclicBarrier barrier = new CyclicBarrier(2);
      wrapInboundInvocationHandler(cache(1), current -> {
         cleanup.add(() -> wrapInboundInvocationHandler(cache(1), ignored -> current));
         return new BarrierHandler(current, barrier);
      });

      CountDownLatch latch = new CountDownLatch(1);
      doAnswer(invocation -> {
         try {
            return invocation.callRealMethod();
         } finally {
            latch.countDown();
         }
      }).when(invocationManager0).notifyCompleted(any(), any());

      cache(0).putAll(Collections.singletonMap(key2, "x"));

      // Notifications are executed in the thread that receives confirmation from backup
      // but the main invoking thread can be released before such follow-up finishes
      assertTrue(latch.await(10, TimeUnit.SECONDS));
      checkDataContainer(dataContainer0, key1, maxBufferedPerSegment > 0, 0);
      checkDataContainer(dataContainer0, key2, true, 0);
      checkDataContainer(dataContainer1, key1, maxBufferedPerSegment > 0, maxBufferedPerSegment/2 + remnant);
      checkDataContainer(dataContainer1, key2, true, maxBufferedPerSegment/2 + 1);

      barrier.await(10, TimeUnit.SECONDS);
      barrier.await(10, TimeUnit.SECONDS);
      checkDataContainer(dataContainer1, key1, maxBufferedPerSegment > 0, 0);
      checkDataContainer(dataContainer1, key2, true, 0);
   }

   private void checkDataContainer(DataContainer dc, MagicKey key, boolean isPresent, int expectedNumRecords) {
      InternalCacheEntry ice = dc.peek(key);
      if (!isPresent) {
         assertNull(ice);
         return;
      }
      assertNotNull(ice);
      Metadata metadata = ice.getMetadata();
      if (metadata == null && expectedNumRecords == 0) return;
      assertNotNull(metadata);
      InvocationRecord records = metadata.lastInvocation();
      int numRecords = records == null ? 0 : records.numRecords();
      assertEquals("Records: " + records, expectedNumRecords, numRecords);
   }

   private class BarrierHandler extends AbstractDelegatingHandler {
      private final CyclicBarrier barrier;

      public BarrierHandler(PerCacheInboundInvocationHandler current, CyclicBarrier barrier) {
         super(current);
         this.barrier = barrier;
      }

      @Override
      protected boolean beforeHandle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         if (command instanceof ForgetInvocationsCommand) {
            try {
               barrier.await(10, TimeUnit.SECONDS);
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
         return super.beforeHandle(command, reply, order);
      }

      @Override
      protected void afterHandle(CacheRpcCommand command, DeliverOrder order, boolean delegated) {
         super.afterHandle(command, order, delegated);
         if (command instanceof ForgetInvocationsCommand) {
            try {
               barrier.await(10, TimeUnit.SECONDS);
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      }
   }
}
