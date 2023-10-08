package org.infinispan.xsite.statetransfer;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Cross-site state transfer status.
 *
 * @author Pedro Ruivo
 * @since 12
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_STATE_TRANSFER_STATUS)
@Proto
public enum StateTransferStatus {
   IDLE,
   SENDING,
   SEND_OK,
   SEND_FAILED,
   SEND_CANCELED;

   public static StateTransferStatus merge(StateTransferStatus one, StateTransferStatus two) {
      switch (one) {
         case IDLE:
            return two;
         case SENDING:
            return two == IDLE ? one : two;
         case SEND_OK:
            switch (two) {
               case IDLE:
               case SENDING:
                  return one;
               default:
                  return two;
            }
         case SEND_FAILED:
            switch (two) {
               case IDLE:
               case SENDING:
               case SEND_OK:
               case SEND_CANCELED:
                  return one;
               default:
                  return two;
            }
         case SEND_CANCELED:
            switch (two) {
               case IDLE:
               case SENDING:
               case SEND_OK:
                  return one;
               default:
                  return two;
            }
         default:
            throw new IllegalStateException();
      }
   }

   public static String toText(StateTransferStatus status) {
      switch (status) {
         case IDLE:
            return "IDLE";
         case SENDING:
            return XSiteStateTransferManager.STATUS_SENDING;
         case SEND_OK:
            return XSiteStateTransferManager.STATUS_OK;
         case SEND_FAILED:
            return XSiteStateTransferManager.STATUS_ERROR;
         case SEND_CANCELED:
            return XSiteStateTransferManager.STATUS_CANCELED;
         default:
            throw new IllegalStateException();
      }
   }
}
