package org.infinispan.remoting.transport.jgroups;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8;
import org.jgroups.Address;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.UUID;

/**
 * Cache JGroupsAddress instances
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class JGroupsAddressCache {
   private static final EquivalentConcurrentHashMapV8<org.jgroups.Address, JGroupsAddress> addressCache =
         new EquivalentConcurrentHashMapV8<>(AnyEquivalence.getInstance(), AnyEquivalence.getInstance());

   // HACK: Avoid the org.jgroups.Address reference in the signature so that local caches can work without the jgroups jar.
   // Otherwise, instantiating the JGroupsAddress externalizer will try to load the org.jgroups.Address class.
   static org.infinispan.remoting.transport.Address fromJGroupsAddress(Object address) {
      final Address addr1 = (Address) address;
      return addressCache.computeIfAbsent(addr1, new EquivalentConcurrentHashMapV8.Fun<Address, JGroupsAddress>() {
         @Override
         public JGroupsAddress apply(Address uuid) {
            if (addr1 instanceof ExtendedUUID)
               return new JGroupsTopologyAwareAddress((ExtendedUUID) addr1);
            else
               return new JGroupsAddress(addr1);
         }
      });
   }

   static void pruneAddressCache() {
      // Prune the JGroups addresses no longer in the UUID cache from the our address cache
      addressCache.forEachKey(Integer.MAX_VALUE, new EquivalentConcurrentHashMapV8.Action<org.jgroups.Address>() {
         @Override
         public void apply(org.jgroups.Address address) {
            if (UUID.get(address) == null) {
               addressCache.remove(address);
            }
         }
      });
   }
}
