package org.infinispan.server.hotrod;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;

/**
 * A Hot Rod server address
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class SingleHomedServerAddress implements ServerAddress {
   private final String host;
   private final int port;

   public SingleHomedServerAddress(String host, int port) {
      this.host = Objects.requireNonNull(host);
      this.port = port;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SingleHomedServerAddress that = (SingleHomedServerAddress) o;

      if (port != that.port) return false;
      return host.equals(that.host);

   }

   @Override
   public int hashCode() {
      int result = host.hashCode();
      result = 31 * result + port;
      return result;
   }

   @Override
   public String toString() {
      return "SingleHomedServerAddress{" + "host='" + host + '\'' + ", port=" + port + '}';
   }

   @Override
   public String getHost(InetAddress localAddress) {
      return host;
   }

   public int getPort() {
      return port;
   }

   static class Externalizer extends AbstractExternalizer<SingleHomedServerAddress> {
      @Override
      public Set<Class<? extends SingleHomedServerAddress>> getTypeClasses() {
         return Collections.singleton(SingleHomedServerAddress.class);
      }

      @Override
      public void writeObject(ObjectOutput output, SingleHomedServerAddress object) throws IOException {
         output.writeObject(object.host);
         output.writeShort(object.port);
      }

      @Override
      public SingleHomedServerAddress readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String host = (String) input.readObject();
         int port = input.readUnsignedShort();
         return new SingleHomedServerAddress(host, port);
      }
   }
}
