package org.infinispan.remoting.transport;

import java.net.InetAddress;

public record PhysicalAddress(InetAddress address, int port) {

   @Override
   public String toString() {
      return address == null ?
            "<unknown>:%d".formatted(port) :
            "%s:%d".formatted(address.getHostAddress(), port);
   }
}
