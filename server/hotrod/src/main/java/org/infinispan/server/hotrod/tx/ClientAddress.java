package org.infinispan.server.hotrod.tx;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.core.ExternalizerIds;

/**
 * A {@link Address} implementation for a client transaction.
 * <p>
 * The {@code localAddress} is the address of the node in which the transaction was replayed.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class ClientAddress implements Address {

   public static final AdvancedExternalizer<ClientAddress> EXTERNALIZER = new Externalizer();
   private final Address localAddress;

   ClientAddress(Address localAddress) {
      this.localAddress = localAddress;
   }

   @Override
   public int compareTo(Address o) {
      if (o instanceof ClientAddress) {
         return localAddress.compareTo(((ClientAddress) o).localAddress);
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
      return localAddress.equals(that.localAddress);
   }

   @Override
   public int hashCode() {
      return localAddress.hashCode();
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

   private static class Externalizer implements AdvancedExternalizer<ClientAddress> {

      @Override
      public Set<Class<? extends ClientAddress>> getTypeClasses() {
         return Collections.singleton(ClientAddress.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CLIENT_ADDRESS;
      }

      @Override
      public void writeObject(ObjectOutput output, ClientAddress object) throws IOException {
         output.writeObject(object.localAddress);
      }

      @Override
      public ClientAddress readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ClientAddress((Address) input.readObject());
      }
   }
}
