package org.infinispan.client.rest;

/**
 * Cross site state transfer mode.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
public enum XSiteStateTransferMode {
   /**
    * Cross-site state transfer is triggered manually via CLI, JMX, or REST.
    */
   MANUAL,
   /**
    * Cross-site state transfer is triggered automatically.
    */
   AUTO;

   private static final XSiteStateTransferMode[] CACHED = XSiteStateTransferMode.values();

   public static XSiteStateTransferMode valueOf(int index) {
      return CACHED[index];
   }
}
