package org.infinispan.test.fwk;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.Discovery;
import org.jgroups.protocols.PingData;
import org.jgroups.protocols.PingHeader;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Promise;
import org.jgroups.util.UUID;

import java.util.Arrays;
import java.util.HashMap;
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
 * NOTE: This protocol implementation requires a new JGroups release cos it
 * relies on sendDiscoveryResponse() method being protected.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class TEST_PING extends Discovery {

   @Property(description="Test name. Default is empty String.")
   private String testName = "";

   private String clusterName;

   private DISCARD discard;

   private boolean discardChecked;

   // Note: Thread locals won't work cos by the time discovery gets around to
   // sending messages, each cluster will be running a different thread.

   // <Test Name, Cluster Name -> <Node Name -> Discovery>>
   private static ConcurrentMap<DiscoveryKey, Map<Address, Discovery>> all =
         new ConcurrentHashMap<DiscoveryKey, Map<Address, Discovery>>();

   public TEST_PING() {
      // Assign a user-defined id for the protocol which must be over 1000
      id = 1320;
   }

   @Override
   public boolean isDynamic() {
      return false;
   }

   @Override
   public void start() throws Exception {
      super.start();
   }

   @Override
   public void stop() {
      super.stop();
      DiscoveryKey key = new DiscoveryKey(testName, clusterName);
      Map<Address, Discovery> discoveries = all.get(key);
      if (discoveries != null) {
         discoveries.remove(local_addr);
         if (discoveries.isEmpty()) {
            boolean removed = all.remove(key, discoveries);
            if (!removed && all.containsKey(key)) {
               throw new IllegalStateException(String.format(
                  "Concurrent discovery removal for test=%s but not removed??",
                  testName));
            }
         }
      } else {
         log.debug(String.format(
            "Test (%s) started but not registered discovery", key));
      }
   }

   @Override
   public void sendGetMembersRequest(String cluster_name, Promise promise,
                                     boolean returnViewsOnly) throws Exception {
      clusterName = cluster_name;

      DiscoveryKey key = new DiscoveryKey(testName, clusterName);
      Map<Address, Discovery> discoveries = all.get(key);
      if (discoveries == null) {
         discoveries = new HashMap<Address, Discovery>();
         Map ret = all.putIfAbsent(key, discoveries);
         if (ret != null)
            discoveries = ret;
      }
      if (log.isTraceEnabled())
         log.trace(String.format("Discoveries for %s are : %s", key, discoveries));

      if (!discoveries.containsKey(local_addr)) {
         discoveries.put(local_addr, this);

         if (log.isTraceEnabled())
            log.trace(String.format(
                  "Add discovery for %s to cache.  The cache now contains: %s",
                  local_addr, discoveries));
      }

      // Only send message if DISCARD is not used, or if DISCARD is
      // configured but it's not discarding messages.
      if (discard == null || !discard.isDiscardAll()) {
         Message msg = createGetMbrsReqMsg(clusterName, returnViewsOnly);
         if (!discoveries.isEmpty()) {
            for (Discovery discovery : discoveries.values()) {
               // Avoid sending to self! Since there are single instances of
               // discovery protocol in each node, just compare them by ref.
               if (discovery != this) {
                  // Rather than relying on transport (PING) or your own multicast
                  // channel (MPING), simply pass the get-members-request to the
                  // discovery protocol instances of the other nodes in the cluster.
                  discovery.up(new Event(Event.MSG, msg));
               }
            }
         } else {
            log.debug("No other nodes yet, so skip sending get-members request");
         }
      } else if (discard != null && discard.isDiscardAll()) {
         log.debug("Not sending discovery because DISCARD is on");
      }
   }

   private Message createGetMbrsReqMsg(String clusterName, boolean returnViewsOnly) {
      PhysicalAddress physical_addr = (PhysicalAddress)
            down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
      List<PhysicalAddress> physical_addrs = Arrays.asList(physical_addr);
      PingData data = new PingData(
            local_addr, null, false, UUID.get(local_addr), physical_addrs);
      PingHeader hdr = new PingHeader(
            PingHeader.GET_MBRS_REQ, data, clusterName);
      hdr.return_view_only = returnViewsOnly;
      Message msg = new Message(null);
      msg.setFlag(Message.OOB);
      msg.setSrc(local_addr);
      msg.putHeader(this.id, hdr);

      if (log.isTraceEnabled())
         log.trace("Create GET_MBRS_REQ message: " + data);

      return msg;
   }

   @Override
   protected void sendDiscoveryResponse(Address logical_addr,
         List<PhysicalAddress> physical_addrs, boolean is_server,
         String logical_name, Address sender) {
      // Not pretty but since this protocol does not rely on the transport, the
      // only possible way to discard messages is by hacking the protocol itself.
      if (!discardChecked) {
         List<Protocol> protocols = getProtocolStack().getProtocols();
         for (Protocol protocol : protocols) {
            if (protocol instanceof DISCARD) {
               discard = (DISCARD) protocol;
               break;
            }
         }
         discardChecked = true;
      }

      if (discard == null || !discard.isDiscardAll()) {
         PingData ping_rsp=new PingData(logical_addr, view, is_server,
                                        logical_name, physical_addrs);
         Message rsp_msg=new Message(sender, logical_addr, null);
         rsp_msg.setFlag(Message.OOB);
         PingHeader rsp_hdr=new PingHeader(PingHeader.GET_MBRS_RSP, ping_rsp);
         rsp_msg.putHeader(this.id, rsp_hdr);

         if(log.isTraceEnabled())
            log.trace(String.format(
                  "%s received GET_MBRS_REQ from %s", this, sender));

         // Instead of sending a get-members-response down the transport,
         // update the cached discovery instances directly.
         DiscoveryKey key = new DiscoveryKey(testName, clusterName);
         Discovery discovery = all.get(key).get(sender);
         if(log.isTraceEnabled())
            log.trace(String.format(
                  "%s sending (dest=%s) response: %s", this, sender, ping_rsp));

         discovery.up(new Event(Event.MSG, rsp_msg));
      }
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

   @Override
   public String toString() {
      return "TEST_PING@" + local_addr;
   }

}
