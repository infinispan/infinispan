package org.infinispan.server.configuration;

/**
 * @since 10.0
 */
public enum AddressType {
   INET_ADDRESS,
   LINK_LOCAL,
   GLOBAL,
   LOOPBACK,
   NON_LOOPBACK,
   SITE_LOCAL,
   MATCH_INTERFACE,
   MATCH_ADDRESS,
   MATCH_HOST;

   public String displayName() {
      return this.toString().replaceAll("_", "-").toLowerCase();
   }
}
