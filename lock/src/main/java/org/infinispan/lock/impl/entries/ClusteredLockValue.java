package org.infinispan.lock.impl.entries;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * Lock object inside the cache. Holds the lock owner, the lock request id, the status of the lock and the nodes that
 * are waiting for the lock to be released.
 * <p>
 * The waiters are not stored as full {@link Address} instances but as a hash of the node UUID, see
 * {@link #waiterId(Address)}. The hash is only used to decide which nodes have to be notified when the lock is
 * released, so a collision is harmless: an extra node is notified and simply ignores the notification because it has
 * no pending request for the lock.
 * <p>
 * Waiters belong to the current ownership epoch. They are dropped as soon as the lock is acquired again, which means
 * that waiters left behind by a timed out, cancelled or crashed request do not need any explicit clean up.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.CLUSTERED_LOCK_VALUE)
public class ClusteredLockValue {

   public static final ClusteredLockValue INITIAL_STATE = new ClusteredLockValue(null, null, ClusteredLockState.RELEASED, null);

   private final String requestId;
   private final Address owner;
   private final ClusteredLockState state;
   private final Set<Integer> waiters;

   @ProtoFactory
   public ClusteredLockValue(String requestId, Address owner, ClusteredLockState state, Set<Integer> waiters) {
      this.requestId = requestId;
      this.owner = owner;
      this.state = state;
      this.waiters = waiters == null || waiters.isEmpty() ? Set.of() : Set.copyOf(waiters);
   }

   /**
    * Computes the identifier used to register a node as a waiter of a lock.
    * <p>
    * The identifier is derived from the node UUID, which makes it stable and identical on every member of the cluster.
    *
    * @param address the node address, must not be {@code null}
    * @return the waiter identifier for the given node
    */
   public static int waiterId(Address address) {
      UUID nodeUUID = address.getNodeUUID();
      long bits = nodeUUID.getMostSignificantBits() ^ nodeUUID.getLeastSignificantBits();
      return (int) (bits >> 32) ^ (int) bits;
   }

   /**
    * Creates the value of a lock that has just been acquired. A new acquisition starts a new ownership epoch, so the
    * waiters registered by the previous owner are discarded.
    *
    * @param requestId  the identifier of the request that acquired the lock
    * @param owner      the node that acquired the lock
    * @param keepWaiter when {@code true}, the new owner stays registered as a waiter because it still has pending
    *                   lock requests that must be notified when this acquisition is released
    * @return the acquired lock value
    */
   public static ClusteredLockValue acquire(String requestId, Address owner, boolean keepWaiter) {
      return new ClusteredLockValue(requestId, owner, ClusteredLockState.ACQUIRED,
            keepWaiter ? Set.of(waiterId(owner)) : null);
   }

   /**
    * Releases the lock keeping the current waiters, so that they can be notified with the released value.
    *
    * @return the released lock value
    */
   public ClusteredLockValue release() {
      return waiters.isEmpty() ? INITIAL_STATE : new ClusteredLockValue(null, null, ClusteredLockState.RELEASED, waiters);
   }

   /**
    * Registers a node as a waiter of this lock.
    *
    * @param waiterId the waiter identifier, see {@link #waiterId(Address)}
    * @return a copy of this value including the waiter, or {@code this} if the node is already registered
    */
   public ClusteredLockValue addWaiter(int waiterId) {
      if (waiters.contains(waiterId))
         return this;

      Set<Integer> updatedWaiters = new HashSet<>(waiters);
      updatedWaiters.add(waiterId);
      return new ClusteredLockValue(requestId, owner, state, updatedWaiters);
   }

   public boolean hasWaiter(int waiterId) {
      return waiters.contains(waiterId);
   }

   @ProtoField(1)
   public String getRequestId() {
      return requestId;
   }

   @ProtoField(2)
   public Address getOwner() {
      return owner;
   }

   @ProtoField(3)
   public ClusteredLockState getState() {
      return state;
   }

   @ProtoField(4)
   public Set<Integer> getWaiters() {
      return waiters;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      ClusteredLockValue that = (ClusteredLockValue) o;
      return Objects.equals(requestId, that.requestId) && Objects.equals(owner, that.owner)
            && Objects.equals(state, that.state) && waiters.equals(that.waiters);
   }

   @Override
   public int hashCode() {
      return Objects.hash(requestId, owner, state, waiters);
   }

   @Override
   public String toString() {
      return "ClusteredLockValue{" +
            " requestId=" + requestId +
            " owner=" + owner +
            " state=" + state +
            " waiters=" + waiters +
            '}';
   }
}
