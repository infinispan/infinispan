package org.infinispan.server.core.security;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Principal;
import java.util.Objects;

/**
 * InetAddressPrincipal.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class InetAddressPrincipal implements Principal {
   private final InetAddress address;

   public InetAddressPrincipal(InetAddress address) {
      if (address == null) {
         throw new IllegalArgumentException("address is null");
     }
     try {
         this.address = InetAddress.getByAddress(address.getHostAddress(), address.getAddress());
     } catch (UnknownHostException e) {
         throw new IllegalStateException(e);
     }
   }

   @Override
   public String getName() {
      return address.getHostAddress();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      InetAddressPrincipal that = (InetAddressPrincipal) o;
      return Objects.equals(address, that.address);
   }

   @Override
   public int hashCode() {
      return Objects.hash(address);
   }

   @Override
   public String toString() {
      return "InetAddressPrincipal [address=" + address + "]";
   }
}
