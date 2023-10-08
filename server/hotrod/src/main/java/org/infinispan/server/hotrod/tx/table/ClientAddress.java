package org.infinispan.server.hotrod.tx.table;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * A {@link Address} implementation for a client transaction.
 * <p>
 * The {@code localAddress} is the address of the node in which the transaction was replayed.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_CLIENT_ADDRESS)
public class ClientAddress implements Address {

   private final Address localAddress;

   public ClientAddress(Address localAddress) {
      this.localAddress = localAddress;
   }

   @ProtoFactory
   ClientAddress(WrappedMessage wrappedLocalAddress) {
      this.localAddress = WrappedMessages.unwrap(wrappedLocalAddress);
   }

   @ProtoField(value = 1, name = "localAddress")
   WrappedMessage getWrappedLocalAddress() {
      return WrappedMessages.orElseNull(localAddress);
   }

   @Override
   public int compareTo(Address o) {
      if (o instanceof ClientAddress) {
         return localAddress == null ?
               (((ClientAddress) o).localAddress == null ? 0 : -1) :
               localAddress.compareTo(((ClientAddress) o).localAddress);
      }
      return -1;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      ClientAddress that = (ClientAddress) o;
      return Objects.equals(localAddress, that.localAddress);
   }

   @Override
   public int hashCode() {
      return localAddress == null ? 0 : localAddress.hashCode();
   }

   @Override
   public String toString() {
      return "ClientAddress{" +
            "localAddress=" + localAddress +
            '}';
   }

   Address getLocalAddress() {
      return localAddress;
   }
}
