package org.infinispan.distribution.ch.impl;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.PersistedConsistentHash;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.remoting.transport.Address;

/**
 * Factory for {@link ConsistentHash} instances.
 *
 * <p>We say a consistent hash {@code ch} is <em>balanced</em> iif {@code rebalance(ch).equals(ch)}.
 *
 * <p>The consistent hashes created by {@link #create(int, int, List, Map)} must be balanced,
 * but the ones created by {@link #updateMembers(ConsistentHash, List, Map)} and
 * {@link #union(ConsistentHash, ConsistentHash)} will likely be unbalanced.
 *
 * @see <a href="https://community.jboss.org/wiki/Non-BlockingStateTransferV2">Non-BlockingStateTransferV2</a>
 *
 * @author Dan Berindei
 * @since 5.2
 */
public interface ConsistentHashFactory<CH extends ConsistentHash> {

   /**
    * Create a new consistent hash instance.
    * The consistent hash will be <em>balanced</em>.
    *
    * @param numOwners The ideal number of owners for each key. The created consistent hash
    *                  can have more or less owners, but each key will have at least one owner.
    * @param numSegments Number of hash-space segments. The implementation may round up the number
    *                    of segments for performance, or may ignore the parameter altogether.
    * @param members A list of addresses representing the new cache members.
    * @param capacityFactors The capacity factor of each member. Determines the relative capacity of each node compared
    *                        to the others. The implementation may ignore this parameter.
    *                        If {@code null}, all the members are assumed to have a capacity factor of 1.
    */
   CH create(int numOwners, int numSegments, List<Address> members, Map<Address, Float> capacityFactors);

   /**
    * Updates an existing consistent hash instance to remove owners that are not in the {@code newMembers} list.
    *
    * <p>If a segment has at least one owner in {@code newMembers}, this method will not add another owner.
    * This guarantees that the new consistent hash can be used immediately, without transferring any state.
    *
    * <p>If a segment has no owners in {@code newMembers} and the {@link ConsistentHash} implementation
    * (e.g. {@link org.infinispan.distribution.ch.impl.DefaultConsistentHash}) requires
    * at least one owner for each segment, this method may add one or more owners for that segment.
    * Since the data in that segment was lost, the new consistent hash can still be used without transferring state.
    *
    * @param baseCH An existing consistent hash instance, should not be {@code null}
    * @param newMembers A list of addresses representing the new cache members.
    * @param capacityFactors The capacity factor of each member. Determines the relative capacity of each node compared
    *                        to the others. The implementation may ignore this parameter.
    *                        If {@code null}, all the members are assumed to have a capacity factor of 1.
    * @return A new {@link ConsistentHash} instance, or {@code baseCH} if the existing instance
    *         does not need any changes.
    */
   CH updateMembers(CH baseCH, List<Address> newMembers, Map<Address, Float> capacityFactors);

   /**
    * Create a new consistent hash instance, based on an existing instance, but <em>balanced</em> according to
    * the implementation's rules.
    *
    * @param baseCH An existing consistent hash instance, should not be {@code null}
    * @return A new {@link ConsistentHash} instance, or {@code baseCH} if the existing instance
    *         does not need any changes.
    */
   CH rebalance(CH baseCH);

   /**
    * Creates a union of two compatible ConsistentHashes (use the same hashing function and have the same configuration
    * parameters).
    *
    * <p>The owners of a segment {@code s} in {@code union(ch1, ch2)} will include both the owners of {@code s}
    * in {@code ch1} and the owners of {@code s} in {@code ch2}, so a cache can switch from using
    * {@code union(ch1, ch2)} to using {@code ch2} without transferring any state.
    */
   CH union(CH ch1, CH ch2);

   /**
    * Recreates a {@link ConsistentHash} from a previously stored persistent state.
    * <p>
    * The stored state typically contains a collection of {@link UUID}s representing the members. If a member is not
    * present when reading from the state, its {@link UUID} must be added to the
    * {@link PersistedConsistentHash#missingUuids()} list. The {@link PersistedConsistentHash#consistentHash()} may have
    * incomplete ownership when there are missing UUIDs.
    *
    * @param state         the state to restore, containing member {@link UUID}s.
    * @param addressMapper A function to map the {@link UUID} to {@link Address}.
    * @return A {@link PersistedConsistentHash} with the {@link ConsistentHash} and with the missing {@link UUID} if
    * any.
    */
   default PersistedConsistentHash<CH> fromPersistentState(ScopedPersistentState state, Function<UUID, Address> addressMapper) {
      throw new UnsupportedOperationException();
   }
}
