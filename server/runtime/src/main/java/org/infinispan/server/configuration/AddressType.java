package org.infinispan.server.configuration;

/**
 * @since 10.0
 */
public enum AddressType {
   ANY_ADDRESS(false),
   INET_ADDRESS(true),
   LINK_LOCAL(false),
   GLOBAL(false),
   LOOPBACK(false),
   NON_LOOPBACK(false),
   SITE_LOCAL(false),
   MATCH_INTERFACE(true),
   MATCH_ADDRESS(true),
   MATCH_HOST(true);

   final boolean value;

   AddressType(boolean value) {
      this.value = value;
   }

   public boolean hasValue() {
      return value;
   }

   @Override
   public String toString() {
      return name().toLowerCase();
   }
}
