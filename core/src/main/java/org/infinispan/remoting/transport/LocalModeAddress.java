package org.infinispan.remoting.transport;

/**
 * Represents the local node's address.
 *
 * @since 9.0
 */
public class LocalModeAddress implements Address {

   public static final Address INSTANCE = new LocalModeAddress();

   private LocalModeAddress() {
   }

   @Override
   public String toString() {
      return "<local>";
   }

   @Override
   public int compareTo(Address o) {
      return o == this ? 0 : -1;
   }
}
