package org.infinispan.test.fwk;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.Discovery;
import org.jgroups.protocols.PingData;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Promise;
import org.jgroups.util.Tuple;
import org.jgroups.util.UUID;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This protocol allows for discovery to happen via data structures maintained
 * in memory rather than relying on JGroup's transport. This allows for
 * discovery to be much faster and predictable because it only relies on java
 * method calls rather than network calls. Clearly, this protocol only works
 * for clusters that are created in memory.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class TEST_PING extends Discovery {

   @Property(description="Test name. Default is empty String.")
   private String testName = "";

   private DISCARD discard;

   // volatile in case resurrection happens from a different thread
   private volatile boolean stopped;

   // Note: Thread locals could work but if two nodes of the same cluster are
   // started from different threads, a thread local based solution would not
   // work, so we're sticking to an static solution

   // <Test Name, Cluster Name -> <Node Name -> Discovery>>
   private static ConcurrentMap<DiscoveryKey, Map<Address, TEST_PING>> all =
         new ConcurrentHashMap<DiscoveryKey, Map<Address, TEST_PING>>();

   static {
      ClassConfigurator.addProtocol((short) 1320, TEST_PING.class);
   }

   @Override
   protected List<PingData> findInitialMembers(Promise<JoinRsp> promise,
         int numExpectedRsps, boolean breakOnCoord, boolean returnViewsOnly) {

      if (!stopped) {
         Map<Address, TEST_PING> discoveries = registerInDiscoveries();

         // Only send message if DISCARD is not used, or if DISCARD is
         // configured but it's not discarding messages.
         boolean discardEnabled = isDiscardEnabled(this);
         if (!discardEnabled) {
            if (!discoveries.isEmpty()) {
               LinkedList<PingData> rsps = new LinkedList<PingData>();
               for (TEST_PING discovery : discoveries.values()) {
                  // Avoid sending to self! Since there are single instances of
                  // discovery protocol in each node, just compare them by ref.
                  boolean traceEnabled = log.isTraceEnabled();
                  if (discovery != this) {
                     boolean remoteDiscardEnabled = isDiscardEnabled(discovery);
                     if (!remoteDiscardEnabled && !discovery.stopped) {
                        addPingRsp(returnViewsOnly, rsps, discovery);
                     } else if (discovery.stopped) {
                        log.debug(String.format(
                              "%s is stopped, so no ping responses will be received",
                              discovery.getLocalAddr()));
                     } else {
                        if (traceEnabled)
                           log.trace("Skipping sending response cos DISCARD is on");
                        return Collections.emptyList();
                     }
                  } else {
                     if (traceEnabled)
                        log.trace("Skipping sending discovery to self");
                  }
               }
               return rsps;
            } else {
               log.debug("No other nodes yet, so skip sending get-members request");
               return Collections.emptyList();
            }
         } else {
            log.debug("Not sending discovery because DISCARD is on");
            return Collections.emptyList();
         }
      } else {
         log.debug("Discovery protocol already stopped, so don't look for members");
         return Collections.emptyList();
      }
   }

   private boolean isDiscardEnabled(TEST_PING discovery) {
      // Not pretty but since this protocol does not rely on the transport, the
      // only possible way to discard messages is by hacking the protocol itself.
      List<Protocol> protocols = discovery.getProtocolStack().getProtocols();
      for (Protocol protocol : protocols) {
         if (protocol instanceof DISCARD) {
            discovery.discard = (DISCARD) protocol;
         }
      }

      return discovery.discard != null && discovery.discard.isDiscardAll();
   }

   private void addPingRsp(boolean returnViewsOnly, LinkedList<PingData> rsps,
                           TEST_PING discovery) {
      // Rather than relying on transport (PING) or your own multicast channel
      // (MPING), talk to other discovery instances directly via Java method
      // calls and discover the other nodes in the cluster.

      // Add mapping of remote's address -> physical addr to the local cache
      mapAddrWithPhysicalAddr(this, discovery);

      // Add mapping of local's address -> physical addr to the remote cache
      mapAddrWithPhysicalAddr(discovery, this);

      Address localAddr = discovery.getLocalAddr();
      List<PhysicalAddress> physicalAddrs = returnViewsOnly ? null :
         Arrays.asList((PhysicalAddress) discovery.down(
               new Event(Event.GET_PHYSICAL_ADDRESS, localAddr)));
      String logicalName = UUID.get(localAddr);
      PingData pingRsp = new PingData(localAddr, discovery.getJGroupsView(),
         discovery.isServer(), logicalName, physicalAddrs);

      if (log.isTraceEnabled())
         log.trace(String.format("Returning ping rsp: %s", pingRsp));

      rsps.add(pingRsp);
   }

   private void mapAddrWithPhysicalAddr(TEST_PING local, TEST_PING remote) {
      PhysicalAddress physical_addr = (PhysicalAddress)
         remote.down(new Event(Event.GET_PHYSICAL_ADDRESS, remote.getLocalAddr()));
      local.down(new Event(Event.SET_PHYSICAL_ADDRESS,
         new Tuple<Address, PhysicalAddress>(remote.getLocalAddr(), physical_addr)));

      if (log.isTraceEnabled())
         log.trace(String.format("Map %s with physical address %s in %s",
                                 remote.getLocalAddr(), physical_addr, local));
   }

   private Map<Address, TEST_PING> registerInDiscoveries() {
      DiscoveryKey key = new DiscoveryKey(testName, group_addr);
      Map<Address, TEST_PING> discoveries = all.get(key);
      if (discoveries == null) {
         discoveries = new HashMap<Address, TEST_PING>();
         Map ret = all.putIfAbsent(key, discoveries);
         if (ret != null)
            discoveries = ret;
      }
      boolean traceEnabled = log.isTraceEnabled();
      if (traceEnabled)
         log.trace(String.format(
               "Discoveries for %s are : %s", key, discoveries));

      if (!discoveries.containsKey(local_addr)) {
         discoveries.put(local_addr, this);

         if (traceEnabled)
            log.trace(String.format(
                  "Add discovery for %s to cache.  The cache now contains: %s",
                  local_addr, discoveries));
      }
      return discoveries;
   }

   @Override
   public void stop() {
      log.debug(String.format("Stop discovery for %s", local_addr));
      super.stop();
      DiscoveryKey key = new DiscoveryKey(testName, group_addr);
      Map<Address, TEST_PING> discoveries = all.get(key);
      if (discoveries != null) {
         removeDiscovery(key, discoveries);
      } else {
         log.debug(String.format(
            "Test (%s) started but not registered discovery", key));
      }
      stopped = true;
   }

   private void removeDiscovery(DiscoveryKey key, Map<Address, TEST_PING> discoveries) {
      discoveries.remove(local_addr);
      if (discoveries.isEmpty()) {
         boolean removed = all.remove(key, discoveries);
         if (!removed && all.containsKey(key)) {
            throw new IllegalStateException(String.format(
               "Concurrent discovery removal for test=%s but not removed??",
               testName));
         }
      }
   }

   protected Address getLocalAddr() {
      return local_addr;
   }

   protected View getJGroupsView() {
      return view;
   }

   protected boolean isServer() {
      return is_server;
   }

   @Override
   public void sendGetMembersRequest(String cluster_name, Promise promise, boolean return_views_only) throws Exception {
      // No-op because it won't get called
   }

   @Override
   public boolean isDynamic() {
      return false;
   }

   @Override
   public String toString() {
      return "TEST_PING@" + local_addr;
   }

   private class DiscoveryKey {
      final String testName;
      final String clusterName;

      private DiscoveryKey(String testName, String clusterName) {
         this.clusterName = clusterName;
         this.testName = testName;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         DiscoveryKey that = (DiscoveryKey) o;

         if (clusterName != null ?
               !clusterName.equals(that.clusterName)
               : that.clusterName != null)
            return false;
         if (testName != null ?
               !testName.equals(that.testName) : that.testName != null)
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = testName != null ? testName.hashCode() : 0;
         result = 31 * result +
               (clusterName != null ? clusterName.hashCode() : 0);
         return result;
      }

      @Override
      public String toString() {
         return "DiscoveryKey{" +
               "clusterName='" + clusterName + '\'' +
               ", testName='" + testName + '\'' +
               '}';
      }
   }

}
