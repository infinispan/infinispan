package org.infinispan.configuration.cache;

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

   private final static XSiteStateTransferMode[] CACHED = XSiteStateTransferMode.values();

   public static XSiteStateTransferMode valueOf(int index) {
      return CACHED[index];
   }
}
