package org.infinispan.client.hotrod;

/**
 * Defines client and protocol version.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class Version {

   private static final String PROTOCOL_VERSION = "2.0";

   public static String getProtocolVersion() {
      return "HotRod client, protocol version :" + PROTOCOL_VERSION;
   }
}
