package org.infinispan.commands;

import java.util.Objects;
import java.util.UUID;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * Uniquely identifies an operation or transaction within the cluster.
 * <p>
 * This identifier is composed of a node's unique identifier and a locally generated, monotonically increasing request
 * ID.
 */
@ProtoTypeId(ProtoStreamTypeIds.REQUEST_UUID)
public final class RequestUUID {

   private static final long LOCAL_BITS = 1;
   private static final UUID LOCAL_UUID = new UUID(LOCAL_BITS, LOCAL_BITS);
   private static final long NO_REQUEST_BITS = 0;
   private static final UUID NO_REQUEST_UUID = new UUID(NO_REQUEST_BITS, NO_REQUEST_BITS);

   /**
    * An empty {@link RequestUUID} used to indicate that an operation does **not** require a unique identifier.
    * <p>
    * This instance contains a zeroed-out UUID (`00000000-0000-0000-0000-000000000000`) and a `requestId` of `0`. It's
    * used as a placeholder when an operation's unique identification is either not applicable or explicitly
    * unnecessary.
    */
   public static RequestUUID NO_REQUEST = new RequestUUID(NO_REQUEST_UUID, NO_REQUEST_BITS);

   private final UUID nodeUUID;
   private final long requestId;

   /**
    * @param nodeUUID  The unique identifier of the node that originated the request.
    * @param requestId A locally incrementing counter, unique within the scope of the `nodeUUID`, used to distinguish
    *                  different operations or transactions from the same node.
    */
   private RequestUUID(UUID nodeUUID, long requestId) {
      this.nodeUUID = Objects.requireNonNull(nodeUUID);
      this.requestId = requestId;
   }

   /**
    * Creates a {@link RequestUUID} for the specified node {@link Address} and request ID. The {@code RequestUUID}'s
    * node identifier will be derived from the provided {@code address}.
    *
    * @param address   The {@link Address} of the node that originated the operation. The
    *                  {@link RequestUUID#nodeUUID nodeUUID} will be obtained from this address.
    * @param requestId The local, monotonically increasing request identifier unique to the node.
    * @return A {@link RequestUUID} representing the given address and request ID.
    */
   public static RequestUUID of(Address address, long requestId) {
      return new RequestUUID(address.getNodeUUID(), requestId);
   }

   /**
    * Creates a {@link RequestUUID} suitable for a single-instance, non-clustered environment. In this context, the
    * {@link RequestUUID#nodeUUID nodeUUID} is {@code null}, signifying that the operation is local and not associated
    * with a specific cluster node.
    *
    * @param requestId The local, monotonically increasing request identifier unique to this single application
    *                  instance.
    * @return A {@link RequestUUID} with a {@code null} node UUID and the provided request ID.
    */
   public static RequestUUID localOf(long requestId) {
      return new RequestUUID(LOCAL_UUID, requestId);
   }

   @ProtoFactory
   static RequestUUID protoFactory(long mostSignificantBits, long leastSignificantBits, long requestId) {
      if (mostSignificantBits == NO_REQUEST_BITS &&
            leastSignificantBits == NO_REQUEST_BITS &&
            requestId == 0) {
         return NO_REQUEST;
      }
      if (mostSignificantBits == LOCAL_BITS &&
            leastSignificantBits == LOCAL_BITS) {
         return localOf(requestId);
      }
      return new RequestUUID(new UUID(mostSignificantBits, leastSignificantBits), requestId);
   }

   @ProtoField(value = 1, defaultValue = "0")
   long getMostSignificantBits() {
      return nodeUUID.getMostSignificantBits();
   }

   @ProtoField(value = 2, defaultValue = "0")
   long getLeastSignificantBits() {
      return nodeUUID.getLeastSignificantBits();
   }

   /**
    * @return The {@link RequestUUID#requestId request identifier}.
    */
   @ProtoField(3)
   public long getRequestId() {
      return requestId;
   }

   /**
    * Returns an {@link Address} representing the origin of this request within the cluster.
    * <p>
    * The returned {@link Address} is a **partial representation** of the originating node. It primarily contains the
    * node's {@link RequestUUID#nodeUUID unique identifier} and is intended for use in routing messages back to the
    * origin through the network.
    * <p>
    * Crucially, this {@link Address} does **not** contain complete topology, version, or other detailed node
    * information. Any fields on the {@link Address} beyond the essential routing identifiers should not be inspected or
    * relied upon, as their values are not guaranteed to be valid or complete.
    * <p>
    * This method will return {@code null} if the {@link RequestUUID} represents an operation from a single-instance,
    * non-clustered environment (i.e., when {@link RequestUUID#localOf(long) localOf} was used).
    *
    * @return The partial {@link Address} instance of the request's originator, or {@code null} if the request
    * originated from a local, non-clustered context.
    */
   public Address toAddress() {
      return nodeUUID == LOCAL_UUID ? null : Address.fromNodeUUID(nodeUUID);
   }

   /**
    * @return A compact representation of this {@link RequestUUID}, containing only the node's logical name and the
    * request ID.
    */
   public String toIdString() {
      return prettyPrintAddress() + ":" + requestId;
   }

   @Override
   public boolean equals(Object obj) {
      if (obj == this) return true;
      if (obj == null || obj.getClass() != this.getClass()) return false;
      var that = (RequestUUID) obj;
      return this.nodeUUID.equals(that.nodeUUID) &&
            this.requestId == that.requestId;
   }

   @Override
   public int hashCode() {
      int result = nodeUUID.hashCode();
      result = 31 * result + Long.hashCode(requestId);
      return result;
   }

   @Override
   public String toString() {
      return "RequestUUID[" +
            "nodeUUID=" + prettyPrintAddress() + ", " +
            "requestId=" + requestId + ']';
   }

   private String prettyPrintAddress() {
      if (nodeUUID == LOCAL_UUID) {
         return "local";
      }
      if (nodeUUID == NO_REQUEST_UUID) {
         return "no-request";
      }
      return Address.nodeUUIDToString(nodeUUID);
   }

}
