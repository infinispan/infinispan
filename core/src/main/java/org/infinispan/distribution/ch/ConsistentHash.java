package org.infinispan.distribution.ch;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

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
    * @return set of node addresses.
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
