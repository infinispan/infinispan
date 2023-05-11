package org.infinispan.statetransfer;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.ControlledRpcManager;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "statetransfer.MergeDuringReplaceTest")
@CleanupAfterMethod
@InCacheMode({ CacheMode.DIST_SYNC })
public class MergeDuringReplaceTest extends MultipleCacheManagersTest {

   private DISCARD[] discard;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(cacheMode, false);
      createClusteredCaches(3, defaultConfig, new TransportFlags().withFD(true).withMerge(true));

      DISCARD d1 = TestingUtil.getDiscardForCache(manager(0));
      DISCARD d2 = TestingUtil.getDiscardForCache(manager(1));
      DISCARD d3 = TestingUtil.getDiscardForCache(manager(2));
      discard = new DISCARD[]{d1, d2, d3};
   }

   public void testMergeDuringReplace() throws Exception {
      final String key = "myKey";
      final String value = "myValue";

      cache(0).put(key, value);

      int nonOwner;
      final Cache<Object, Object> c;
      LocalizedCacheTopology cacheTopology = advancedCache(0).getDistributionManager().getCacheTopology();
      List<Address> members = new ArrayList<>(cacheTopology.getMembers());
      List<Address> owners = cacheTopology.getDistribution(key).readOwners();
      members.removeAll(owners);
      nonOwner = cacheTopology.getMembers().indexOf(members.get(0));
      c = cache(nonOwner);

      List<Cache<Object, Object>> partition1 = caches();
      partition1.remove(c);

      ControlledRpcManager controlledRpcManager = ControlledRpcManager.replaceRpcManager(c);
      controlledRpcManager.excludeCommands(StateTransferStartCommand.class, StateResponseCommand.class);

      Future<Boolean> future = fork(() -> c.replace(key, value, "myNewValue"));

      ControlledRpcManager.BlockedRequest blockedReplace = controlledRpcManager.expectCommand(ReplaceCommand.class);

      discard[nonOwner].discardAll(true);

      // wait for the partitions to form
      TestingUtil.blockUntilViewsReceived(30000, false, partition1.get(0), partition1.get(1));
      TestingUtil.blockUntilViewsReceived(30000, false, c);
      TestingUtil.waitForNoRebalance(partition1.get(0), partition1.get(1));
      TestingUtil.waitForNoRebalance(c);

      blockedReplace.send().receiveAll();

      // Since the non owner didn't have the value before the split it can't do the replace correctly
      assertEquals(future.get(10, TimeUnit.SECONDS), Boolean.FALSE);

      controlledRpcManager.stopBlocking();
   }

   public int findNonOwner(String key) {
      for (Cache cache : caches()) {
         if (!cache.getAdvancedCache().getDataContainer().containsKey(key)) {
            return caches().indexOf(cache);
         }
      }
      throw new IllegalStateException();
   }
}
