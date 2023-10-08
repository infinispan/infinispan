package org.infinispan.remoting.responses;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
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
public class UnsureResponse extends ValidResponse {
   public static final UnsureResponse INSTANCE = new UnsureResponse();

   @ProtoFactory
   static UnsureResponse protoFactory(MarshallableObject<?> object, MarshallableCollection<?> collection,
                                      MarshallableMap<?, ?> map, MarshallableArray<?> array) {
      return INSTANCE;
   }

   @Override
   public boolean isSuccessful() {
      return false;
   }

   @Override
   protected boolean isReturnValueDisabled() {
      return true;
   }
}
