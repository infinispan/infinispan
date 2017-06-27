package org.infinispan.partitionhandling;

import static org.infinispan.test.TestingUtil.extractComponentRegistry;
import static org.testng.Assert.assertEquals;

import org.infinispan.Cache;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.PartitionStatusChanged;
import org.infinispan.notifications.cachelistener.event.PartitionStatusChangedEvent;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.test.Exceptions;
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
      // Keys stay available in between the availability mode update and the topology update
      StateTransferManager stmP0N1 = extractComponentRegistry(cache(p0.node(1))).getStateTransferManager();
      eventuallyEquals(2, () -> stmP0N1.getCacheTopology().getActualMembers().size());
      assertEquals(AvailabilityMode.AVAILABLE, partitionHandlingManager(p0.node(0)).getAvailabilityMode());

      // The availability didn't change on p0.node0, check that keys owned by p1 are not accessible
      partition(0).assertKeyAvailableForRead(k0Existing, "v0");
      partition(0).assertKeysNotAvailableForRead(k1Existing, k2Existing);
      // k3 is an exception, since p0.node0 is a backup owner it returns the local value
      assertPartiallyAvailable(p0, k3Existing, "v3");

      partition(0).assertKeyAvailableForRead(k0Missing, null);
      partition(0).assertKeysNotAvailableForRead(k1Missing, k2Missing);
      // k3 is an exception, since p0.node0 is a backup owner it returns the local value
      assertPartiallyAvailable(p0, k3Missing, null);

      // Allow the partition handling manager on p0.node0 to update the availability mode
      ss.exit("main:check_availability");

      partition(0).assertDegradedMode();
      partition(1).assertDegradedMode();
   }

   private void assertPartiallyAvailable(PartitionDescriptor p0, Object k3Existing, Object value) {
      assertEquals(value, cache(p0.node(0)).get(k3Existing));
      Exceptions.expectException(AvailabilityException.class, () -> cache(p0.node(1)).get(k3Existing));
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
