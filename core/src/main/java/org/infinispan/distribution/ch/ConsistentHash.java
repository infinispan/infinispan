package org.infinispan.distribution.ch;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import org.infinispan.distribution.ch.impl.ConsistentHashFactory;
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
    * @return The actual number of hash space segments. Note that it may not be the same as the number
    *         of segments passed in at creation time.
    */
   int getNumSegments();

   /**
    * Should return the addresses of the nodes used to create this consistent hash.
    *
    * @return list of node addresses.
    */
   List<Address> getMembers();

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
    * @return a non-null set of segment IDs, may or may not be unmodifiable, which shouldn't be modified by caller.
    * The set is empty if {@code owner} is not a member of the consistent hash.
    */
   Set<Integer> getSegmentsForOwner(Address owner);

   /**
    * Returns the segments that this cache member is the primary owner for.
    * @param owner the address of the member
    * @return a non-null set of segment IDs, may or may not be unmodifiable, which shouldn't be modified by caller.
    * The set is empty if {@code owner} is not a member of the consistent hash.
    */
   Set<Integer> getPrimarySegmentsForOwner(Address owner);

   /**
    * Returns a string containing all the segments and their associated addresses.
    */
   String getRoutingTableAsString();

   /**
    * Writes this {@link ConsistentHash} to the specified scoped persistent state.
    *
    * @param state         The state to which this {@link ConsistentHash} will be written.
    * @param addressMapper The mapper {@link Function} to convert the {@link Address} to the {@link UUID} used to
    *                      persist the address within the state.
    */
   default void toScopedState(ScopedPersistentState state, Function<Address, UUID> addressMapper) {
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
