package org.infinispan.client.hotrod;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class Version {

   private static final String PROTOCOL_VERSION = "1.0"; 

   public static String getProtocolVersion() {
      return "HotRod client, protocol version :" + PROTOCOL_VERSION;
   }
}
