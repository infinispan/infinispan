package org.infinispan.server.resp.commands.cluster;

import java.util.List;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.commands.FamilyCommand;
import org.jgroups.stack.IpAddress;

public class CLUSTER extends FamilyCommand {

   private static final RespCommand[] CLUSTER_COMMANDS;

   static {
      CLUSTER_COMMANDS = new RespCommand[] {
            new SHARDS(),
            new NODES(),
            new SLOTS(),
      };
   }

   public CLUSTER() {
      super(-2, 0, 0, 0);
   }

   @Override
   public RespCommand[] getFamilyCommands() {
      return CLUSTER_COMMANDS;
   }

   public static Address findPhysicalAddress(EmbeddedCacheManager ecm) {
      Transport transport = ecm.getTransport();
      if (transport == null) {
         return null;
      }

      List<Address> addresses = transport.getPhysicalAddresses();
      if (addresses.isEmpty()) {
         // Returning a logical address.
         return ecm.getAddress();
      }
      return addresses.get(0);
   }

   public static int findPort(Address address) {
      int port = 0;
      if (address instanceof JGroupsAddress && ((JGroupsAddress) address).getJGroupsAddress() instanceof IpAddress) {
         JGroupsAddress jAddress = (JGroupsAddress) address;
         port = ((IpAddress) jAddress.getJGroupsAddress()).getPort();
      }
      return port;
   }

   public static String getOnlyIp(Address address) {
      if (address instanceof JGroupsAddress && ((JGroupsAddress) address).getJGroupsAddress() instanceof IpAddress) {
         JGroupsAddress jAddress = (JGroupsAddress) address;
         return ((IpAddress) jAddress.getJGroupsAddress()).getIpAddress().getHostAddress();
      }
      return address.toString();
   }
}
