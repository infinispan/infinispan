package org.infinispan.remoting.transport.jgroups;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jgroups.Address;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.NameCache;

/**
 * Cache JGroupsAddress instances
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class JGroupsAddressCache {
   private static final ConcurrentMap<Address, JGroupsAddress> addressCache =
         new ConcurrentHashMap<>();

   public static org.infinispan.remoting.transport.Address fromJGroupsAddress(Address jgroupsAddress) {
      // New entries are rarely added added after startup, but computeIfAbsent synchronizes every time
      JGroupsAddress ispnAddress = addressCache.get(jgroupsAddress);
      if (ispnAddress != null) {
         return ispnAddress;
      }
      return addressCache.computeIfAbsent(jgroupsAddress, uuid -> {
         if (jgroupsAddress instanceof ExtendedUUID) {
            return new JGroupsTopologyAwareAddress((ExtendedUUID) jgroupsAddress);
         } else {
            return new JGroupsAddress(jgroupsAddress);
         }
      });
   }

   static void pruneAddressCache() {
      // Prune the JGroups addresses & LocalUUIDs no longer in the UUID cache from the our address cache
      addressCache.forEach((address, ignore) -> {
         if (NameCache.get(address) == null) {
            addressCache.remove(address);
         }
      });
   }
}
