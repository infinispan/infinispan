package org.infinispan.remoting.transport.jgroups;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.remoting.transport.Address;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.NameCache;

/**
 * Cache {@link Address} instances
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class AddressCache {
   private static final Map<ExtendedUUID, Address> addressCache = new ConcurrentHashMap<>();

   public static Address fromExtendedUUID(ExtendedUUID addr) {
      // New entries are rarely added after startup, but computeIfAbsent synchronizes every time
      var existing = addressCache.get(addr);
      return existing == null ?
            addressCache.computeIfAbsent(addr, Address::fromExtendedUUID) :
            existing;
   }

   public static Address getIfPresent(long mostSignificantBits, long leastSignificantBits) {
      return addressCache.get(new ExtendedUUID(mostSignificantBits, leastSignificantBits));
   }

   static void pruneAddressCache() {
      // Prune the JGroups addresses & LocalUUIDs no longer in the UUID cache from the our address cache
      addressCache.keySet().removeIf(addr -> NameCache.get(addr) == null);
   }
}
