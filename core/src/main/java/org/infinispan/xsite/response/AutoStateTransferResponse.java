package org.infinispan.xsite.response;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.responses.Response;
import org.infinispan.xsite.commands.XSiteAutoTransferStatusCommand;

/**
 * A {@link Response} implementation for command {@link XSiteAutoTransferStatusCommand}.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_AUTO_STATE_TRANSFER_RESPONSE)
public class AutoStateTransferResponse implements Response {

   private static final XSiteStateTransferMode[] CACHED_VALUES = XSiteStateTransferMode.values();

   @ProtoField(value = 1, defaultValue = "false")
   final boolean isOffline;
   @ProtoField(2)
   final XSiteStateTransferMode stateTransferMode;

   @ProtoFactory
   public AutoStateTransferResponse(boolean isOffline, XSiteStateTransferMode stateTransferMode) {
      this.isOffline = isOffline;
      this.stateTransferMode = stateTransferMode;
   }

   private static XSiteStateTransferMode valueOf(int ordinal) {
      return CACHED_VALUES[ordinal];
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isValid() {
      return true;
   }

   public boolean isOffline() {
      return isOffline;
   }

   public XSiteStateTransferMode stateTransferMode() {
      return stateTransferMode;
   }

   public boolean canDoAutomaticStateTransfer() {
      return isOffline && stateTransferMode == XSiteStateTransferMode.AUTO;
   }
}
