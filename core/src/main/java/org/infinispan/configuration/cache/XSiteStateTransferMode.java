package org.infinispan.configuration.cache;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Cross site state transfer mode.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
@Proto
@ProtoTypeId(ProtoStreamTypeIds.XSITE_STATE_TRANSFER_MODE)
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
