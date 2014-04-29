package org.infinispan.server.core.security;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Principal;

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
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((address == null) ? 0 : address.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      InetAddressPrincipal other = (InetAddressPrincipal) obj;
      if (address == null) {
         if (other.address != null)
            return false;
      } else if (!address.equals(other.address))
         return false;
      return true;
   }

   @Override
   public String toString() {
      return "InetAddressPrincipal [address=" + address + "]";
   }
}
