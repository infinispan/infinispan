package org.infinispan.commands;

import java.util.Objects;
import java.util.UUID;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * Uniquely identifies an operation or transaction within the cluster.
 * <p>
 * This identifier is composed of a node's unique identifier and a locally generated, monotonically increasing request
 * ID.
 *
 * @param nodeUUID  The unique identifier of the node that originated the request.
 * @param requestId A locally incrementing counter, unique within the scope of the `nodeUUID`, used to distinguish
 *                  different operations or transactions from the same node.
 */
@ProtoTypeId(ProtoStreamTypeIds.REQUEST_UUID)
@Proto
public record RequestUUID(UUID nodeUUID, long requestId) {

   /**
    * An empty {@link RequestUUID} used to indicate that an operation does **not** require a unique identifier.
    * <p>
    * This instance contains a zeroed-out UUID (`00000000-0000-0000-0000-000000000000`) and a `requestId` of `0`. It's
    * used as a placeholder when an operation's unique identification is either not applicable or explicitly
    * unnecessary.
    */
   public static RequestUUID NO_REQUEST = new RequestUUID(new UUID(0, 0), 0);

   /**
    * Creates a {@link RequestUUID} for the specified node {@link Address} and request ID. The {@code RequestUUID}'s
    * node identifier will be derived from the provided {@code address}.
    *
    * @param address   The {@link Address} of the node that originated the operation. The
    *                  {@link RequestUUID#nodeUUID() nodeUUID} will be obtained from this address.
    * @param requestId The local, monotonically increasing request identifier unique to the node.
    * @return A {@link RequestUUID} representing the given address and request ID.
    */
   public static RequestUUID of(Address address, long requestId) {
      return new RequestUUID(address.getNodeUUID(), requestId);
   }

   /**
    * Creates a {@link RequestUUID} suitable for a single-instance, non-clustered environment. In this context, the
    * {@link RequestUUID#nodeUUID() nodeUUID} is {@code null}, signifying that the operation is local and not associated
    * with a specific cluster node.
    *
    * @param requestId The local, monotonically increasing request identifier unique to this single application
    *                  instance.
    * @return A {@link RequestUUID} with a {@code null} node UUID and the provided request ID.
    */
   public static RequestUUID localOf(long requestId) {
      return new RequestUUID(null, requestId);
   }

   /**
    * Returns an {@link Address} representing the origin of this request within the cluster.
    * <p>
    * The returned {@link Address} is a **partial representation** of the originating node. It primarily contains the
    * node's {@link RequestUUID#nodeUUID() unique identifier} and is intended for use in routing messages back to the
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
      return nodeUUID == null ? null : Address.fromNodeUUID(nodeUUID);
   }

   /**
    * Creates a new {@link RequestUUID} instance by replacing this record's
    * {@link RequestUUID#nodeUUID() node identifier} with the provided {@code otherNodeUUID}, while retaining the
    * original {@link RequestUUID#requestId() request identifier}.
    * <p>
    * This method is typically used when an operation, identified by its local {@code requestId}, is being processed or
    * re-assigned to a different node due to a node failure. It allows tracking the same logical operation (via
    * {@code requestId}) but associating it with a new originating or processing node (via {@code otherNodeUUID}).
    *
    * @param otherNodeUUID The {@link UUID} of the node that this new {@link RequestUUID} should represent as its
    *                      originator.
    * @return A new {@link RequestUUID} with the updated node UUID and the original request ID.
    */
   public RequestUUID asNodeUUID(UUID otherNodeUUID) {
      return new RequestUUID(otherNodeUUID, requestId);
   }

   /**
    * @return A compact representation of this {@link RequestUUID}, containing only the node's logical name and the
    * request ID.
    */
   public String toIdString() {
      return Objects.toString(toAddress(), "local") + ":" + requestId;
   }
}
