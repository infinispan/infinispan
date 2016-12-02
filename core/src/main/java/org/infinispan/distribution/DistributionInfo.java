package org.infinispan.distribution;

import java.util.Collection;
import java.util.List;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;

public class DistributionInfo {
   private final Address primary;
   private final List<Address> owners;
   private final Ownership ownership;

   public DistributionInfo(Object key, ConsistentHash ch, Address self) {
      int segment = ch.getSegment(key);
      if (ch.isReplicated()) {
         owners = ch.getMembers();
         primary = ch.locatePrimaryOwnerForSegment(segment);
         // Even in replicated mode a node may be non-owner during state transfer
         ownership = primary.equals(self) ? Ownership.PRIMARY :
                     owners.contains(self) ? Ownership.BACKUP : Ownership.NON_OWNER;
      } else {
         owners = ch.locateOwnersForSegment(segment);
         int index = owners.indexOf(self);
         if (index == 0) {
            ownership = Ownership.PRIMARY;
            primary = self;
         } else if (index > 0) {
            ownership = Ownership.BACKUP;
            primary = owners.get(0);
         } else {
            ownership = Ownership.NON_OWNER;
            primary = owners.get(0);
         }
      }
   }

   public boolean isPrimary() {
      return ownership == Ownership.PRIMARY;
   }

   public Address primary() {
      return primary;
   }

   public Ownership ownership() {
      return ownership;
   }

   public Collection<Address> owners() {
      return owners;
   }

   public Collection<Address> backups() {
      return owners.subList(0, owners.size());
   }
}
