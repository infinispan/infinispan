package org.infinispan.lock.impl.entries;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;

/**
 * Lock object inside the cache. Holds the lock owner, the lock request id and the status of the lock.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.CLUSTERED_LOCK_VALUE)
public class ClusteredLockValue {

   public static final ClusteredLockValue INITIAL_STATE = new ClusteredLockValue(null, null, ClusteredLockState.RELEASED);
   private final String requestId;
   private final Address owner;
   private final ClusteredLockState state;

   public ClusteredLockValue(String requestId, Address owner, ClusteredLockState state) {
      this.requestId = requestId;
      this.owner = owner;
      this.state = state;
   }

   @ProtoFactory
   ClusteredLockValue(String requestId, JGroupsAddress owner, ClusteredLockState state) {
      this(requestId, (Address) owner, state);
   }

   @ProtoField(1)
   public String getRequestId() {
      return requestId;
   }

   @ProtoField(value = 2, javaType = JGroupsAddress.class)
   public Address getOwner() {
      return owner;
   }

   @ProtoField(3)
   public ClusteredLockState getState() {
      return state;
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
      return Objects.equals(requestId, that.requestId) && Objects.equals(owner, that.owner) && Objects.equals(state, that.state);
   }

   @Override
   public int hashCode() {
      return Objects.hash(requestId, owner, state);
   }

   @Override
   public String toString() {
      return "ClusteredLockValue{" +
            " requestId=" + requestId +
            " owner=" + owner +
            " state=" + state +
            '}';
   }
}
