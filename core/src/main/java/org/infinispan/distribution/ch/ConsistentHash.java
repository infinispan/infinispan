package org.infinispan.distribution.ch;

import org.infinispan.commons.hash.Hash;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A consistent hash algorithm implementation. Implementations would typically be constructed via a
 * {@link ConsistentHashFactory}.
 *
 * A consistent hash assigns each key a list of owners; the number of owners is defined at creation time,
 * but the consistent hash is free to return a smaller or a larger number of owners, depending on
 * circumstances, as long as each key has at least one owner.
 *
 * The first element in the list of owners is the "primary owner". A key will always have a primary owner.
 * The other owners are called "backup owners".
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
    * @return The configured number of owners for each key. Note that {code @getOwners(key)} may return
    *         a different number of owners.
    */
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
    */
   default Address locatePrimaryOwner(Object key) {
      return locatePrimaryOwnerForSegment(getSegment(key));
   }

   /**
    * Finds all the owners of a key. The first element in the returned list is the primary owner.
    *
    * @param key key to locate
    * @return An unmodifiable list of addresses where the key resides.
    *         Will never be {@code null}, and it will always have at least 1 element.
    */
   default List<Address> locateOwners(Object key) {
      return locateOwnersForSegment(getSegment(key));
   }

   default Set<Address> locateAllOwners(Collection<Object> keys) {
      // Use a HashSet assuming most of the time the number of keys is small.
      HashSet<Address> owners = new HashSet<Address>();
      HashSet<Integer> segments = new HashSet<Integer>();
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
    */
   default boolean isKeyLocalToNode(Address nodeAddress, Object key) {
      return locateOwnersForSegment(getSegment(key)).contains(nodeAddress);
   }

   /**
    * @return The hash space segment that a key maps to.
    */
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
    * @return a non-null set of segment IDs
    */
   Set<Integer> getSegmentsForOwner(Address owner);

   /**
    * Returns the segments that this cache member is the primary owner for.
    * @param owner the address of the member
    * @return a non-null set of segment IDs
    */
   Set<Integer> getPrimarySegmentsForOwner(Address owner);

   /**
    * Returns a string containing all the segments and their associated addresses.
    */
   String getRoutingTableAsString();

   /**
     * Writes this ConsistentHash to the specified scoped state.
     *
     * @param state the state to which this ConsistentHash will be written
     */
   default void toScopedState(ScopedPersistentState state) {
      throw new UnsupportedOperationException();
   }
}
