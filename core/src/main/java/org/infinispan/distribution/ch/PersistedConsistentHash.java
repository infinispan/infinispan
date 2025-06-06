package org.infinispan.distribution.ch;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import org.infinispan.distribution.ch.impl.ConsistentHashFactory;
import org.infinispan.globalstate.ScopedPersistentState;

/**
 * A record containing the {@link ConsistentHash} restored from persistent state and any missing
 * {@link UUID}s.
 *
 * @param consistentHash The {@link ConsistentHash} instance. Note that its ownership information might be incomplete if
 *                       {@link #missingUuids()} is not empty.
 * @param missingUuids   A collection of {@link UUID}s representing members that were present in the
 *                       persistent state but are not currently online.
 * @param <CH>           The specific implementation type of the {@link ConsistentHash}.
 * @see ConsistentHashFactory#fromPersistentState(ScopedPersistentState, Function)
 */
public record PersistedConsistentHash<CH extends ConsistentHash>(CH consistentHash,
                                                                 Collection<UUID> missingUuids) {

   public PersistedConsistentHash(CH consistentHash, Collection<UUID> missingUuids) {
      this.consistentHash = Objects.requireNonNull(consistentHash);
      this.missingUuids = Set.copyOf(missingUuids);
   }

   /**
    * Returns the total number of members that were present in the persistent state, including those currently marked as
    * missing.
    *
    * @return The total number of members, including both online and missing members.
    */
   public int totalMembers() {
      return consistentHash.getMembers().size() + missingUuids.size();
   }

   /**
    * Indicates whether there are missing members that were part of the persistent state but are not currently online.
    *
    * @return {@code true} if the {@link #missingUuids()} collection is not empty, signifying potential incomplete
    * ownership information in the associated {@link ConsistentHash}. Otherwise, returns {@code false}.
    */
   public boolean hasMissingMembers() {
      return !missingUuids.isEmpty();
   }
}
