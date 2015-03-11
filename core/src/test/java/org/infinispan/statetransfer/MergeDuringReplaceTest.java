package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

@Test(groups = "functional", testName = "statetransfer.MergeDuringReplaceTest")
@CleanupAfterMethod
public class MergeDuringReplaceTest extends MultipleCacheManagersTest {

   private DISCARD[] discard;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      createClusteredCaches(3, defaultConfig, new TransportFlags().withFD(true).withMerge(true));

      DISCARD d1 = TestingUtil.getDiscardForCache(cache(0));
      d1.setExcludeItself(true);
      DISCARD d2 = TestingUtil.getDiscardForCache(cache(1));
      d2.setExcludeItself(true);
      DISCARD d3 = TestingUtil.getDiscardForCache(cache(2));
      d3.setExcludeItself(true);
      discard = new DISCARD[]{d1, d2, d3};
   }

   public void testMergeDuringReplace() throws Exception {
      final String key = "myKey";
      final String value = "myValue";

      cache(0).put(key, value);

      ConsistentHash ch = cache(0).getAdvancedCache().getComponentRegistry()
            .getStateTransferManager().getCacheTopology().getReadConsistentHash();
      List<Address> members = new ArrayList<>(ch.getMembers());
      List<Address> owners = ch.locateOwners(key);
      members.removeAll(owners);
      int nonOwner = ch.getMembers().indexOf(members.get(0));
      final Cache<Object, Object> c = cache(nonOwner);

      List<Cache<Object, Object>> partition1 = caches();
      partition1.remove(c);

      ControlledRpcManager controlledRpcManager = new ControlledRpcManager(c.getAdvancedCache().getRpcManager());
      TestingUtil.replaceComponent(c, RpcManager.class, controlledRpcManager, true);

      controlledRpcManager.blockBefore(ReplaceCommand.class);

      Future<Boolean> future = fork(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            return c.replace(key, value, "myNewValue");
         }
      });

      discard[nonOwner].setDiscardAll(true);

      // wait for the partitions to form
      TestingUtil.blockUntilViewsReceived(30000, false, partition1.get(0), partition1.get(1));
      TestingUtil.blockUntilViewsReceived(30000, false, c);
      TestingUtil.waitForRehashToComplete(partition1.get(0), partition1.get(1));
      TestingUtil.waitForRehashToComplete(c);

      controlledRpcManager.stopBlocking();

      // Since the non owner didn't have the value before the split it can't do the replace correctly
      assertEquals(future.get(10, TimeUnit.SECONDS), Boolean.FALSE);
   }
}
