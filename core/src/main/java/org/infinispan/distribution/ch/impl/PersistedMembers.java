package org.infinispan.distribution.ch.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.PersistentUUID;

/**
 * A record containing the currently online members' {@link Address}es, their capacity factors, and the
 * {@link PersistentUUID}s of any members that were part of a previous state but are not currently online.
 *
 * @param members         The list of {@link Address}es of the currently online members.
 * @param capacityFactors A map of {@link Address} to their capacity factor. This may be {@code null} if all online
 *                        members have a default capacity factor of 1.
 * @param missingUuids    A collection of {@link PersistentUUID}s representing members that were present in a previous
 *                        persistent state but are not currently online.
 */
record PersistedMembers(List<Address> members, Map<Address, Float> capacityFactors,
                        Collection<PersistentUUID> missingUuids) {

   public PersistedMembers(List<Address> members, Map<Address, Float> capacityFactors, Collection<PersistentUUID> missingUuids) {
      this.members = List.copyOf(members);
      this.capacityFactors = capacityFactors == null ? null : Map.copyOf(capacityFactors);
      this.missingUuids = Set.copyOf(missingUuids);
   }
}
