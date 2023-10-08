package org.infinispan.server.hotrod;

import java.net.InetAddress;
import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A Hot Rod server address
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_SINGLE_HOMED_SERVER_ADDRESS)
public class SingleHomedServerAddress implements ServerAddress {

   @ProtoField(1)
   final String host;

   @ProtoField(value = 2, defaultValue = "-1")
   final int port;

   @ProtoFactory
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
}
