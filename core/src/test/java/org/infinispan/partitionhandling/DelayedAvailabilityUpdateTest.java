package org.infinispan.partitionhandling;

import static org.infinispan.test.Exceptions.expectExecutionException;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.PartitionStatusChanged;
import org.infinispan.notifications.cachelistener.event.PartitionStatusChangedEvent;
import org.infinispan.test.concurrent.StateSequencer;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.DelayedAvailabilityUpdateTest")
public class DelayedAvailabilityUpdateTest extends BasePartitionHandlingTest {
   public void testDelayedAvailabilityUpdate0() throws Exception {
      testDelayedAvailabilityUpdate(new PartitionDescriptor(0, 1), new PartitionDescriptor(2, 3));
   }

   public void testDelayedAvailabilityUpdate1() throws Exception {
      testDelayedAvailabilityUpdate(new PartitionDescriptor(0, 2), new PartitionDescriptor(1, 3));
   }

   public void testDelayedAvailabilityUpdate2() throws Exception {
      testDelayedAvailabilityUpdate(new PartitionDescriptor(0, 3), new PartitionDescriptor(1, 2));
   }

   public void testDelayedAvailabilityUpdate3() throws Exception {
      testDelayedAvailabilityUpdate(new PartitionDescriptor(1, 2), new PartitionDescriptor(0, 3));
   }

   public void testDelayedAvailabilityUpdate4() throws Exception {
      testDelayedAvailabilityUpdate(new PartitionDescriptor(1, 3), new PartitionDescriptor(0, 2));
   }

   public void testDelayedAvailabilityUpdate5() throws Exception {
      testDelayedAvailabilityUpdate(new PartitionDescriptor(2, 3), new PartitionDescriptor(0, 1));
   }

   protected void testDelayedAvailabilityUpdate(PartitionDescriptor p0, PartitionDescriptor p1) throws Exception {
      Object k0Existing = new MagicKey("k0Existing", cache(p0.node(0)), cache(p0.node(1)));
      Object k1Existing = new MagicKey("k1Existing", cache(p0.node(1)), cache(p1.node(0)));
      Object k2Existing = new MagicKey("k2Existing", cache(p1.node(0)), cache(p1.node(1)));
      Object k3Existing = new MagicKey("k3Existing", cache(p1.node(1)), cache(p0.node(0)));
      Object k0Missing = new MagicKey("k0Missing", cache(p0.node(0)), cache(p0.node(1)));
      Object k1Missing = new MagicKey("k1Missing", cache(p0.node(1)), cache(p1.node(0)));
      Object k2Missing = new MagicKey("k2Missing", cache(p1.node(0)), cache(p1.node(1)));
      Object k3Missing = new MagicKey("k3Missing", cache(p1.node(1)), cache(p0.node(0)));

      Cache<Object, Object> cacheP0N0 = cache(p0.node(0));
      cacheP0N0.put(k0Existing, "v0");
      cacheP0N0.put(k1Existing, "v1");
      cacheP0N0.put(k2Existing, "v2");
      cacheP0N0.put(k3Existing, "v3");

      StateSequencer ss = new StateSequencer();
      ss.logicalThread("main", "main:block_availability_update_p0n0", "main:after_availability_update_p0n1",
            "main:check_availability", "main:resume_availability_update_p0n0");

      log.debugf("Delaying the availability mode update on node %s", address(p0.node(0)));
      cache(p0.node(0)).addListener(new BlockAvailabilityChangeListener(true, ss,
            "main:block_availability_update_p0n0", "main:resume_availability_update_p0n0"));
      cache(p0.node(1)).addListener(new BlockAvailabilityChangeListener(false, ss,
            "main:after_availability_update_p0n1"));

      splitCluster(p0.getNodes(), p1.getNodes());

      ss.enter("main:check_availability");
      // Receive the topology update on p0.node1, block it on p1.node0
      DistributionManager dmP0N1 = advancedCache(p0.node(1)).getDistributionManager();
      eventuallyEquals(2, () -> dmP0N1.getCacheTopology().getActualMembers().size());
      assertEquals(AvailabilityMode.AVAILABLE, partitionHandlingManager(p0.node(0)).getAvailabilityMode());

      // Reads for k0* and k3* succeed because they're local and p0.node0 cache is available
      assertKeyAvailableForRead(cacheP0N0, k0Existing, "v0");
      assertKeyAvailableForRead(cacheP0N0, k3Existing, "v3");
      partition(0).assertKeyAvailableForRead(k0Missing, null);
      assertKeyAvailableForRead(cacheP0N0, k3Missing, null);

      // Reads for k1* fail immediately because they are sent to p0.node1, which is already degraded
      assertKeyNotAvailableForRead(cacheP0N0, k1Existing);
      assertKeyNotAvailableForRead(cacheP0N0, k1Missing);

      // Reads for k2 wait for the topology update before failing
      // because all read owners are suspected, but the new topology could add read owners
      Future<Object> getK2Existing = fork(() -> cacheP0N0.get(k2Existing));
      Future<Map<Object, Object>> getAllK2Existing =
         fork(() -> cacheP0N0.getAdvancedCache().getAll(Collections.singleton(k2Existing)));
      Future<Object> getK2Missing = fork(() -> cacheP0N0.get(k2Missing));
      Future<Map<Object, Object>> getAllK2Missing =
         fork(() -> cacheP0N0.getAdvancedCache().getAll(Collections.singleton(k2Missing)));
      Thread.sleep(50);
      assertFalse(getK2Existing.isDone());
      assertFalse(getAllK2Existing.isDone());
      assertFalse(getK2Missing.isDone());
      assertFalse(getAllK2Missing.isDone());

      // Allow the partition handling manager on p0.node0 to update the availability mode
      ss.exit("main:check_availability");

      partition(0).assertDegradedMode();
      partition(1).assertDegradedMode();

      expectExecutionException(AvailabilityException.class, getK2Existing);
      expectExecutionException(AvailabilityException.class, getAllK2Existing);
      expectExecutionException(AvailabilityException.class, getK2Missing);
      expectExecutionException(AvailabilityException.class, getAllK2Missing);
   }

   @Listener
   public static class BlockAvailabilityChangeListener {
      private boolean blockPre;
      private StateSequencer ss;
      private String[] states;

      BlockAvailabilityChangeListener(boolean blockPre, StateSequencer ss, String... states) {
         this.blockPre = blockPre;
         this.ss = ss;
         this.states = states;
      }

      @PartitionStatusChanged
      public void onPartitionStatusChange(PartitionStatusChangedEvent e) throws Exception {
         if (blockPre == e.isPre()) {
            for (String state : states) {
               ss.advance(state);
            }
         }
      }
   }
}
