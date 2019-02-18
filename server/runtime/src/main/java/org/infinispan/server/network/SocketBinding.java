package org.infinispan.server.network;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class SocketBinding {
   private final String name;
   private final NetworkAddress address;
   private final int port;

   public SocketBinding(String name, NetworkAddress networkAddress, int port) {
      this.name = name;
      this.address = networkAddress;
      this.port = port;
   }

   public NetworkAddress getAddress() {
      return address;
   }

   public int getPort() {
      return port;
   }

   public String getName() {
      return name;
   }

   public String toString() {
      return "SocketBinding{" +
            "name='" + name + '\'' +
            ", address=" + address +
            ", port=" + port +
            '}';
   }
}
