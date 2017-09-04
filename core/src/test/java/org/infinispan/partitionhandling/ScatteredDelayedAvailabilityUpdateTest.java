package org.infinispan.partitionhandling;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BiasAcquisition;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.concurrent.StateSequencer;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.infinispan.test.TestingUtil.extractComponentRegistry;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;
import static org.testng.AssertJUnit.assertFalse;

@Test(groups = "functional", testName = "partitionhandling.DelayedAvailabilityUpdateTest")
public class ScatteredDelayedAvailabilityUpdateTest extends DelayedAvailabilityUpdateTest {
   {
      cacheMode = CacheMode.SCATTERED_SYNC;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new ScatteredDelayedAvailabilityUpdateTest().biasAcquisition(BiasAcquisition.NEVER),
            new ScatteredDelayedAvailabilityUpdateTest().biasAcquisition(BiasAcquisition.ON_WRITE)
      };
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
      // Availability update and topology update are not atomic on p0n1, in order to prevent sending reads in different
      // topologies we have to wait for the topology update as well
      ss.logicalThread("main", "main:block_availability_update_p0n0", "main:after_availability_update_p0n1",
            "main:check_before_topology_update_p0n1", "main:resume_topology_update_p0n1", "main:check_availability",
            "main:resume_availability_update_p0n0");

      log.debugf("Delaying the availability mode update on node %s", address(p0.node(0)));
      cache(p0.node(0)).addListener(new BlockAvailabilityChangeListener(true, ss,
            "main:block_availability_update_p0n0", "main:resume_availability_update_p0n0"));
      cache(p0.node(1)).addListener(new BlockAvailabilityChangeListener(false, ss,
            "main:after_availability_update_p0n1", "main:resume_topology_update_p0n1"));

      int topologyBeforeSplit = TestingUtil.extractComponent(cache(p0.node(1)), StateTransferManager.class).getCacheTopology().getTopologyId();
      splitCluster(p0.getNodes(), p1.getNodes());

      ss.enter("main:check_before_topology_update_p0n1");
      // Now we should get topology 8 which just drops the primary owners but keeps the cache available, and soon
      // afterwards topology 9 which actually carries the degraded mode info. If topology 9 arrives before 8
      // the availability update will be blocked, but in topology 9 is blocked on p0n0 and therefore the reads
      // below would block.
      int currentTopology = TestingUtil.extractComponent(cache(p0.node(1)), StateTransferManager.class).getCacheTopology().getTopologyId();
      if (currentTopology == topologyBeforeSplit + 1) {
         assertKeyNotAvailableForRead(cache(p0.node(1)), k0Existing);
         assertKeyNotAvailableForRead(cache(p0.node(1)), k0Missing);
      }
      CompletableFuture<Void> topologyUpdateFuture = TestingUtil.extractComponent(cache(p0.node(1)), StateTransferLock.class).topologyFuture(currentTopology + 1);
      ss.exit("main:check_before_topology_update_p0n1");

      topologyUpdateFuture.get(10, TimeUnit.SECONDS);

      ss.enter("main:check_availability");
      // Keys stay available in between the availability mode update and the topology update
      StateTransferManager stmP0N1 = extractComponentRegistry(cache(p0.node(1))).getStateTransferManager();
      eventuallyEquals(2, () -> stmP0N1.getCacheTopology().getActualMembers().size());
      assertEquals(AvailabilityMode.AVAILABLE, partitionHandlingManager(p0.node(0)).getAvailabilityMode());
      // p0n1 should be degraded at this momment

      ArrayList<Check> checks = new ArrayList<>();
      assertKeyAvailableForRead(cache(p0.node(0)), k0Existing, "v0");
      // p0n1 has progressed to newer topology; in that case the read for k0existing will be sent
      // with higher topology than the one on p0n0 (the availability & topology update is blocked) and
      // it will block on p0n0.
      checks.add(new Check("p0n1 k0Existing", fork(() -> assertKeyNotAvailableForRead(cache(p0.node(1)), k0Existing))));
      if (biasAcquisition == BiasAcquisition.NEVER) {
         // owner (p0.node(1)) has already become degraded so we get exception from it
         assertKeyNotAvailableForRead(cache(p0.node(0)), k1Existing);
      } else {
         // we have the bias and read the value from local (available) node
         assertKeyAvailableForRead(cache(p0.node(0)), k1Existing, "v1");
      }
      assertKeyNotAvailableForRead(cache(p0.node(1)), k1Existing);

      // We find out that the other partition is not reachable, but we don't know why
      // - that's why PartitionHandlingInterceptor waits for topology update or degraded mode.
      // However the update is blocked by the test
      if (biasAcquisition == BiasAcquisition.NEVER) {
         checks.add(new Check("p0n0 k2Existing", fork(() -> assertKeyNotAvailableForRead(cache(p0.node(0)), k2Existing))));
         checks.add(new Check("p0n0 k3Existing", fork(() -> assertKeyNotAvailableForRead(cache(p0.node(0)), k3Existing))));
      } else {
         assertKeyAvailableForRead(cache(p0.node(0)), k2Existing, "v2");
         assertKeyAvailableForRead(cache(p0.node(0)), k3Existing, "v3");
      }
      assertKeyNotAvailableForRead(cache(p0.node(1)), k2Existing);
      assertKeyNotAvailableForRead(cache(p0.node(1)), k3Existing);

      assertKeyAvailableForRead(cache(p0.node(0)), k0Missing, null);
      // assertKeyNotAvailableForRead(cache(p0.node(1)), k0Missing);
      checks.add(new Check("p0n1 k0Missing", fork(() -> assertKeyNotAvailableForRead(cache(p0.node(1)), k0Missing))));
      // with bias on write, we haven't written the missing key so the node does not have the bias and this will fail
      assertKeyNotAvailableForRead(cache(p0.node(0)), k1Missing);
      assertKeyNotAvailableForRead(cache(p0.node(1)), k1Missing);

      checks.add(new Check("p0n0 k2Missing", fork(() -> assertKeyNotAvailableForRead(cache(p0.node(0)), k2Missing))));
      checks.add(new Check("p0n0 k3Missing", fork(() -> assertKeyNotAvailableForRead(cache(p0.node(0)), k3Missing))));
      assertKeyNotAvailableForRead(cache(p0.node(1)), k2Missing);
      assertKeyNotAvailableForRead(cache(p0.node(1)), k3Missing);

      // None of the p0n0's checks should be finished
      Thread.sleep(100);
      for (Check check : checks) {
         assertFalse(check.description + " has completed", check.f.isDone());
      }

      // Allow the partition handling manager on p0.node0 to update the availability mode
      ss.exit("main:check_availability");

      for (Check check : checks) {
         try {
            check.f.get(10, TimeUnit.SECONDS);
         } catch (TimeoutException e) {
            fail(check.description + " timed out");
         } catch (Exception e) {
            log.error(check.description, e);
            fail(check.description + " " + e.getMessage());
         }
      }

      partition(0).assertDegradedMode();
      partition(1).assertDegradedMode();
   }

   private static class Check {
      private final String description;
      private final Future<?> f;

      private Check(String description, Future<?> f) {
         this.description = description;
         this.f = f;
      }
   }
}
