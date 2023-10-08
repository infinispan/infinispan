package org.infinispan.remoting.responses;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * An unsuccessful response
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.UNSUCCESSFUL_RESPONSE)
public class UnsuccessfulResponse extends ValidResponse {
   public static final UnsuccessfulResponse EMPTY = new UnsuccessfulResponse(null, null, null, null);

   @ProtoFactory
   UnsuccessfulResponse(MarshallableObject<?> object, MarshallableCollection<?> collection,
                        MarshallableMap<?, ?> map, MarshallableArray<?> array) {
      super(object, collection, map, array);
   }

   public static UnsuccessfulResponse create(Object value) {
      return value == null ? EMPTY : new UnsuccessfulResponse(new MarshallableObject<>(value), null, null, null);
   }

   @Override
   public boolean isSuccessful() {
      return false;
   }
}
