package org.infinispan.remoting.responses;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * An unsure response - used with Dist - essentially asks the caller to check the next response from the next node since
 * the sender is in a state of flux (probably in the middle of rebalancing)
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.UNSURE_RESPONSE)
public class UnsureResponse implements ValidResponse<Void> {
   public static final UnsureResponse INSTANCE = new UnsureResponse();

   @ProtoFactory
   static UnsureResponse protoFactory() {
      return INSTANCE;
   }

   @Override
   public boolean isSuccessful() {
      return false;
   }

   @Override
   public Void getResponseValue() {
      throw new UnsupportedOperationException();
   }
}
