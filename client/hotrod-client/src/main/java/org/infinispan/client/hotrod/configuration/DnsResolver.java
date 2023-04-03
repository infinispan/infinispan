package org.infinispan.client.hotrod.configuration;

/**
 * @since 15.0
 **/
public enum DnsResolver {
   /**
    * Uses the default DNS resolver
    */
   DEFAULT,
   /**
    * A resolver that supports random selection of destination addresses if multiple are provided by the nameserver. This is ideal
    * for use in applications that use a pool of connections, for which connecting to a single resolved address would be
    * inefficient.
    */
   ROUND_ROBIN,
   /**
    * Used in combination with a {@link org.infinispan.client.hotrod.TransportFactory} allows the use of any valid Netty DNS resolver.
    */
   CUSTOM
}
