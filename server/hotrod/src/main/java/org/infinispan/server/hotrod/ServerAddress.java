package org.infinispan.server.hotrod;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.remoting.transport.Address;

/**
 * A Hot Rod server address
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class ServerAddress implements Address {
   private final String host;
   private final int port;

   public ServerAddress(String host, int port) {
      this.host = Objects.requireNonNull(host);
      this.port = port;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ServerAddress that = (ServerAddress) o;

      if (port != that.port) return false;
      return host.equals(that.host);

   }

//   // IMPORTANT NOTE: Hot Rod protocol agrees to this calculation for a node
//   // address hash code calculation, so any changes to the implementation
//   // require modification of the protocol.
//   override def hashCode() = Arrays.hashCode(
//      "%s:%d".format(host, port).getBytes(UTF8))

   @Override
   public int hashCode() {
      int result = host.hashCode();
      result = 31 * result + port;
      return result;
   }

   @Override
   public int compareTo(Address o) {
      if (o instanceof ServerAddress) {
         ServerAddress oa = (ServerAddress) o;
         int cmp = host.compareTo(oa.host);
         if (cmp == 0) {
            cmp = port - oa.port;
         }
         return cmp;
      }
      return -1;
   }

   @Override
   public String toString() {
      final StringBuffer sb = new StringBuffer("ServerAddress{");
      sb.append("host='").append(host).append('\'');
      sb.append(", port=").append(port);
      sb.append('}');
      return sb.toString();
   }

   public String getHost() {
      return host;
   }

   public int getPort() {
      return port;
   }

   static class Externalizer extends AbstractExternalizer<ServerAddress> {
      @Override
      public Set<Class<? extends ServerAddress>> getTypeClasses() {
         return Collections.singleton(ServerAddress.class);
      }

      @Override
      public void writeObject(ObjectOutput output, ServerAddress object) throws IOException {
         output.writeObject(object.host);
         output.writeShort(object.port);
      }

      @Override
      public ServerAddress readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String host = (String) input.readObject();
         int port = input.readUnsignedShort();
         return new ServerAddress(host, port);
      }
   }
}