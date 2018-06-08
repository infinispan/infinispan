package org.infinispan.distribution.ch;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.remoting.transport.Address;

/**
 * A consistent hash algorithm implementation. Implementations would typically be constructed via a
 * {@link ConsistentHashFactory}.
 *
 * A consistent hash assigns each key a list of owners; the number of owners is defined at creation time,
 * but the consistent hash is free to return a smaller or a larger number of owners, depending on
 * circumstances.
 *
 * The first element in the list of owners is the "primary owner". The other owners are called "backup owners".
 * Some implementations guarantee that there will always be a primary owner, others do not.
 *
 * This interface gives access to some implementation details of the consistent hash.
 *
 * Our consistent hashes work by splitting the hash space (the set of possible hash codes) into
 * fixed segments and then assigning those segments to nodes dynamically. The number of segments
 * is defined at creation time, and the mapping of keys to segments never changes.
 * The mapping of segments to nodes can change as the membership of the cache changes.
 *
 * Normally application code doesn't need to know about this implementation detail, but some
 * applications may benefit from the knowledge that all the keys that map to one segment are
 * always located on the same server.
 *
 * @see <a href="https://community.jboss.org/wiki/Non-BlockingStateTransferV2">Non-BlockingStateTransferV2</a>
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author Dan Berindei
 * @author anistor@redhat.com
 * @since 4.0
 */
public interface ConsistentHash {
   /**
    * @return The configured number of owners. Note that the actual number of owners of each key may be different.
    * @deprecated Since 9.1, it should not be used to obtain the number of owners of a particular key.
    */
   @Deprecated
   int getNumOwners();

   /**
    * @deprecated Since 8.2, the {@code Hash} is optional - replaced in the configuration by the
    * {@code KeyPartitioner}
    */
   @Deprecated
   default Hash getHashFunction() {
      throw new UnsupportedOperationException();
   }

   /**
    * @return The actual number of hash space segments. Note that it may not be the same as the number
    *         of segments passed in at creation time.
    */
   int getNumSegments();

   /**
    * Should return the addresses of the nodes used to create this consistent hash.
    *
    * @return set of node addresses.
    */
   List<Address> getMembers();

   /**
    * Should be equivalent to return the first element of {@link #locateOwners}.
    * Useful as a performance optimization, as this is a frequently needed information.
    * @param key key to locate
    * @return the address of the owner
    * @deprecated Since 9.0, please use {@link LocalizedCacheTopology#getDistribution(Object)} instead.
    */
   @Deprecated
   default Address locatePrimaryOwner(Object key) {
      return locatePrimaryOwnerForSegment(getSegment(key));
   }

   /**
    * Finds all the owners of a key. The first element in the returned list is the primary owner.
    *
    * @param key key to locate
    * @return An unmodifiable list of addresses where the key resides.
    *         Will never be {@code null}, and it will always have at least 1 element.
    * @deprecated Since 9.0, please use {@link LocalizedCacheTopology#getDistribution(Object)} instead.
    */
   @Deprecated
   default List<Address> locateOwners(Object key) {
      return locateOwnersForSegment(getSegment(key));
   }

   /**
    * @deprecated Since 9.0, please use {@link LocalizedCacheTopology#getWriteOwners(Collection)} instead.
    */
   @Deprecated
   default Set<Address> locateAllOwners(Collection<Object> keys) {
      // Use a HashSet assuming most of the time the number of keys is small.
      HashSet<Address> owners = new HashSet<>();
      IntSet segments = IntSets.mutableEmptySet(getNumSegments());
      for (Object key : keys) {
         int segment = getSegment(key);
         if (segments.add(segment)) {
            owners.addAll(locateOwnersForSegment(segment));
         }
         if (owners.size() == getMembers().size()) {
            return owners;
         }
      }
      return owners;
   }

   /**
    * Test to see whether a key is owned by a given node.
    *
    * @param nodeAddress address of the node to test
    * @param key key to test
    * @return {@code true} if the key is mapped to the address; {@code false} otherwise
    * @deprecated Since 9.0, please use {@link LocalizedCacheTopology#isReadOwner(Object)} and {@link LocalizedCacheTopology#isWriteOwner(Object)} instead.
    */
   @Deprecated
   default boolean isKeyLocalToNode(Address nodeAddress, Object key) {
      return locateOwnersForSegment(getSegment(key)).contains(nodeAddress);
   }

   /**
    * @return The hash space segment that a key maps to.
    *
    * @deprecated Since 9.0, please use {@link KeyPartitioner#getSegment(Object)}
    *    or {@link LocalizedCacheTopology#getSegment(Object)} instead.
    */
   @Deprecated
   int getSegment(Object key);

   /**
    * @return All the nodes that own a given hash space segment, first address is the primary owner. The returned list is unmodifiable.
    */
   List<Address> locateOwnersForSegment(int segmentId);

   /**
    * @return The primary owner of a given hash space segment. This is equivalent to {@code locateOwnersForSegment(segmentId).get(0)} but is more efficient
    */
   Address locatePrimaryOwnerForSegment(int segmentId);

   /**
    * Check if a segment is local to a given member.
    *
    * <p>Implementation note: normally key-based method are implemented based on segment-based methods.
    * Here, however, we need a default implementation for the segment-based method for
    * backwards-compatibility reasons.</p>
    *
    * @since 8.2
    */
   default boolean isSegmentLocalToNode(Address nodeAddress, int segmentId) {
      return locateOwnersForSegment(segmentId).contains(nodeAddress);
   }

   /**
    * @return {@code true} if every member owns every segment. This allows callers to skip computing the
    * segment of a key in some cases.
    */
   default boolean isReplicated() {
      // Returning true is only an optimization, so it's ok to return false by default.
      return false;
   }

   /**
    * Returns the segments owned by a cache member.
    *
    * @param owner the address of the member
    * @return a non-null set of segment IDs, may or may not be unmodifiable, which shouldn't be modified by caller
    */
   Set<Integer> getSegmentsForOwner(Address owner);

   /**
    * Returns the segments that this cache member is the primary owner for.
    * @param owner the address of the member
    * @return a non-null set of segment IDs, may or may not be unmodifiable, which shouldn't be modified by caller
    */
   Set<Integer> getPrimarySegmentsForOwner(Address owner);

   /**
    * Returns a string containing all the segments and their associated addresses.
    */
   String getRoutingTableAsString();

   /**
     * Writes this ConsistentHash to the specified scoped state. Before invoking this method, the ConsistentHash
     * addresses will have to be replaced with their corresponding {@link org.infinispan.topology.PersistentUUID}s
     *
     * @param state the state to which this ConsistentHash will be written
     */
   default void toScopedState(ScopedPersistentState state) {
      throw new UnsupportedOperationException();
   }

   /**
    * Returns a new ConsistentHash with the addresses remapped according to the provided {@link UnaryOperator}.
    * If an address cannot me remapped (i.e. the remapper returns null) this method should return null.
    *
    * @param remapper the remapper which given an address replaces it with another one
    * @return the remapped ConsistentHash or null if one of the remapped addresses is null
    */
   default ConsistentHash remapAddresses(UnaryOperator<Address> remapper) {
      throw new UnsupportedOperationException();
   }

   /**
    * The capacity factor of each member. Determines the relative capacity of each node compared to the others.
    * If {@code null}, all the members are assumed to have a capacity factor of 1.
    */
   default Map<Address, Float> getCapacityFactors() {
      return null;
   }
}
