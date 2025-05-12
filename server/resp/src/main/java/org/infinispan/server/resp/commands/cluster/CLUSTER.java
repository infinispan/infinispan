package org.infinispan.server.resp.commands.cluster;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.PhysicalAddress;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.commands.FamilyCommand;

public class CLUSTER extends FamilyCommand {

   private static final RespCommand[] CLUSTER_COMMANDS;

   static {
      CLUSTER_COMMANDS = new RespCommand[] {
            new SHARDS(),
            new NODES(),
            new SLOTS(),
            new KEYSLOT(),
      };
   }

   public CLUSTER() {
      super(-2, 0, 0, 0, AclCategory.SLOW.mask());
   }

   @Override
   public RespCommand[] getFamilyCommands() {
      return CLUSTER_COMMANDS;
   }

   public static PhysicalAddress findPhysicalAddress(EmbeddedCacheManager ecm) {
      Transport transport = SecurityActions.getGlobalComponentRegistry(ecm).getComponent(Transport.class);
      if (transport == null) {
         return null;
      }

      var addresses = transport.getPhysicalAddresses();
      return addresses.isEmpty() ? null : addresses.get(0);
   }

   static IntSet ownedSlots(Address member, ConsistentHash ch, SegmentSlotRelation ssr) {
      IntSet slots = IntSets.mutableEmptySet();
      for (int segment: ch.getPrimarySegmentsForOwner(member)) {
         slots.addAll(ssr.segmentToSlots(segment));
      }
      return IntSets.immutableSet(slots);
   }
}
