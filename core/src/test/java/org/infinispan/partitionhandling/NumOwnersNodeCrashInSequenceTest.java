package org.infinispan.partitionhandling;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partionhandling.AvailabilityException;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.concurrent.CommandMatcher;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.test.concurrent.StateSequencerUtil;
import org.infinispan.test.fwk.TEST_PING;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.Channel;
import org.jgroups.View;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * With a cluster made out of nodes {A,B,C,D}, tests that D crashes and before the state transfer finishes, another node
 * C crashes. {A,B} should enter in degraded mode. The only way in which it could recover is explicitly, through JMX
 * operations.
 */
@Test(groups = "functional", testName = "partitionhandling.NumOwnersNodeCrashInSequenceTest")
public class NumOwnersNodeCrashInSequenceTest extends MultipleCacheManagersTest {

   private static Log log = LogFactory.getLog(NumOwnersNodeCrashInSequenceTest.class);

   ControlledConsistentHashFactory cchf = new ControlledConsistentHashFactory(new int[]{0, 1}, new int[]{1, 2},
                                                                              new int[]{2, 3}, new int[]{3, 0});

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      configBuilder.clustering().partitionHandling().enabled(true);
      configBuilder.clustering().hash().numSegments(4).stateTransfer().timeout(2000);
      configBuilder.clustering().hash().consistentHashFactory(cchf);
      createCluster(configBuilder, 4);
      waitForClusterToForm();
   }

   public void testNodeCrashedBeforeStFinished() throws Exception {
      Object k0 = new MagicKey(cache(0), cache(1));
      Object k1 = new MagicKey(cache(0), cache(1));
      Object k2 = new MagicKey(cache(1), cache(2));
      Object k3 = new MagicKey(cache(1), cache(2));
      Object k4 = new MagicKey(cache(2), cache(3));
      Object k5 = new MagicKey(cache(2), cache(3));
      Object k6 = new MagicKey(cache(3), cache(0));
      Object k7 = new MagicKey(cache(3), cache(0));

      final Object[] allKeys = new Object[] {k0, k1, k2, k3, k4, k5, k6, k7};
      for (Object k : allKeys) cache(new Random().nextInt(4)).put(k, k);

      StateSequencer ss = new StateSequencer();
      ss.logicalThread("main", "main:st_in_progress", "main:2nd_node_left", "main:cluster_unavailable");

      cchf.setOwnerIndexes(new int[]{0, 1}, new int[]{1, 2}, new int[]{2, 1}, new int[]{2, 0});

      final StateTransferManager stm0 = advancedCache(0).getComponentRegistry().getStateTransferManager();
      final int initialTopologyId = stm0.getCacheTopology().getTopologyId();
      StateSequencerUtil.advanceOnInboundRpc(ss, manager(1), new CommandMatcher() {
         @Override
         public boolean accept(ReplicableCommand command) {
            System.out.println("command = " + command);
            if (!(command instanceof StateResponseCommand))
               return false;
            StateResponseCommand responseCommand = (StateResponseCommand) command;
            System.out.println(responseCommand.getTopologyId() + " ==? " + (initialTopologyId + 2));
            return responseCommand.getTopologyId() == initialTopologyId + 2;
         }
      }).before("main:st_in_progress", "main:cluster_unavailable");

      log.trace("Before killing manager3");
      Address missing = address(3);
      crashCacheManagers(manager(3));
      installNewView(advancedCache(0).getRpcManager().getTransport().getMembers(), missing, manager(0), manager(1), manager(2));
      ss.enter("main:2nd_node_left");
      log.trace("Killing 2nd node");
      missing = address(2);
      crashCacheManagers(manager(2));
      installNewView(advancedCache(0).getRpcManager().getTransport().getMembers(), missing, manager(0), manager(1));
      ss.exit("main:2nd_node_left");

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            log.trace("Testing condition");
            for (Object k : allKeys) {
               try {
                  cache(0).get(k);
                  return false;
               } catch (AvailabilityException e) {
               }
               try {
                  cache(1).put(k, k);
                  return false;
               } catch (AvailabilityException e) {
               }
            }
            return true;
         }
      });
   }

   private void installNewView(List<Address> members, Address missing, EmbeddedCacheManager... where) {
      log.tracef("installNewView:members=%s, missing=%s", members, missing);
      final List<org.jgroups.Address> viewMembers = new ArrayList<org.jgroups.Address>();
      for (Address a : members)
         if (!a.equals(missing))
            viewMembers.add(getjGroupsAddress((JGroupsAddress) a));
      int viewId = where[0].getGlobalComponentRegistry().getComponent(Transport.class).getViewId() + 1;
      View view = View.create(viewMembers.get(0), viewId, (org.jgroups.Address[]) viewMembers.toArray(new org.jgroups.Address[viewMembers.size()]));

      log.trace("Before installing new view:" + viewMembers);
      for (EmbeddedCacheManager ecm : where) {
         Channel c = ((JGroupsTransport) ecm.getGlobalComponentRegistry().getComponent(Transport.class)).getChannel();
         ((GMS) c.getProtocolStack().findProtocol(GMS.class)).installView(view);
      }
   }

   private org.jgroups.Address getjGroupsAddress(JGroupsAddress a) {
      return a.getJGroupsAddress();
   }

   /**
    * Simulates a node crash, discarding all the messages from/to this node and then stopping the caches.
    */
   private static void crashCacheManagers(EmbeddedCacheManager... cacheManagers) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         JGroupsTransport t = (JGroupsTransport) cm.getGlobalComponentRegistry().getComponent(Transport.class);
         Channel channel = t.getChannel();
         try {
            DISCARD discard = new DISCARD();
            discard.setDiscardAll(true);
            channel.getProtocolStack().insertProtocol(discard, ProtocolStack.ABOVE, TP.class);
         } catch (Exception e) {
            log.warn("Problems inserting discard", e);
            throw new RuntimeException(e);
         }
         View view = View.create(channel.getAddress(), 100);
         ((GMS) channel.getProtocolStack().findProtocol(GMS.class)).installView(view);
         TEST_PING protocol = (TEST_PING) channel.getProtocolStack().findProtocol(TEST_PING.class);
         if (protocol != null) protocol.suspend();
      }
      TestingUtil.killCacheManagers(cacheManagers);
   }

}
