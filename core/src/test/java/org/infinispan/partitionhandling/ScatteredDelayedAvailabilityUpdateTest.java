package org.infinispan.partitionhandling;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.test.concurrent.StateSequencer;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.concurrent.Future;

import static org.infinispan.test.TestingUtil.extractComponentRegistry;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

@Test(groups = "functional", testName = "partitionhandling.DelayedAvailabilityUpdateTest")
public class ScatteredDelayedAvailabilityUpdateTest extends DelayedAvailabilityUpdateTest {
   {
      cacheMode = CacheMode.SCATTERED_SYNC;
   }

   @Override
   protected void testDelayedAvailabilityUpdate(PartitionDescriptor p0, PartitionDescriptor p1) throws Exception {
      Object k0Existing = new MagicKey("k0Existing", cache(p0.node(0)));
      Object k1Existing = new MagicKey("k1Existing", cache(p0.node(1)));
      Object k2Existing = new MagicKey("k2Existing", cache(p1.node(0)));
      Object k3Existing = new MagicKey("k3Existing", cache(p1.node(1)));
      Object k0Missing = new MagicKey("k0Missing", cache(p0.node(0)));
      Object k1Missing = new MagicKey("k1Missing", cache(p0.node(1)));
      Object k2Missing = new MagicKey("k2Missing", cache(p1.node(0)));
      Object k3Missing = new MagicKey("k3Missing", cache(p1.node(1)));

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
      // p0n1 should be degraded at this momment

      assertKeyAvailableForRead(cache(p0.node(0)), k0Existing, "v0");
      assertKeyNotAvailableForRead(cache(p0.node(1)), k0Existing);
      // owner (p0.node(0)) has already become degraded so we get exception from it
      assertKeyNotAvailableForRead(cache(p0.node(0)), k1Existing);
      assertKeyNotAvailableForRead(cache(p0.node(1)), k1Existing);

      // We find out that the other partition is not reachable, but we don't know why
      // - that's why PartitionHandlingInterceptor waits for topology update or degraded mode.
      // However the update is blocked by the test
      ArrayList<Future<?>> checks = new ArrayList<>();
      checks.add(fork(() -> assertKeyNotAvailableForRead(cache(p0.node(0)), k2Existing)));
      checks.add(fork(() -> assertKeyNotAvailableForRead(cache(p0.node(0)), k3Existing)));
      assertKeyNotAvailableForRead(cache(p0.node(1)), k2Existing);
      assertKeyNotAvailableForRead(cache(p0.node(1)), k3Existing);

      assertKeyAvailableForRead(cache(p0.node(0)), k0Missing, null);
      // owner (p0.node(0)) has already become degraded so we get exception from it
      assertKeyNotAvailableForRead(cache(p0.node(1)), k0Missing);
      assertKeyNotAvailableForRead(cache(p0.node(0)), k1Missing);
      assertKeyNotAvailableForRead(cache(p0.node(1)), k1Missing);

      checks.add(fork(() -> assertKeyNotAvailableForRead(cache(p0.node(0)), k2Missing)));
      checks.add(fork(() -> assertKeyNotAvailableForRead(cache(p0.node(0)), k3Missing)));
      assertKeyNotAvailableForRead(cache(p0.node(1)), k2Missing);
      assertKeyNotAvailableForRead(cache(p0.node(1)), k3Missing);

      // None of the p0n0's checks should be finished
      Thread.sleep(100);
      for (Future<?> check : checks) {
         assertFalse(check.isDone());
      }

      // Allow the partition handling manager on p0.node0 to update the availability mode
      ss.exit("main:check_availability");

      for (Future<?> check : checks) {
         check.get();
      }

      partition(0).assertDegradedMode();
      partition(1).assertDegradedMode();
   }
}

