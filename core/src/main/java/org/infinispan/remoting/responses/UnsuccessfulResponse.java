package org.infinispan.remoting.responses;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * An unsuccessful response
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.UNSUCCESSFUL_RESPONSE)
public class UnsuccessfulResponse<T> implements ValidResponse<T> {
   public static final UnsuccessfulResponse<Object> EMPTY_RESPONSE = new UnsuccessfulResponse<>(null);

   @ProtoField(1)
   final MarshallableObject<T> object;

   @ProtoFactory
   @SuppressWarnings("unchecked")
   static <T> UnsuccessfulResponse<T> protoFactory(MarshallableObject<T> object) {
      return object == null ? (UnsuccessfulResponse<T>) EMPTY_RESPONSE : new UnsuccessfulResponse<>(object);
   }

   UnsuccessfulResponse(T object) {
      this.object = MarshallableObject.create(object);
   }

   UnsuccessfulResponse(MarshallableObject<T> object) {
      this.object = object;
   }

   @Override
   public T getResponseValue() {
      return MarshallableObject.unwrap(object);
   }

   @Override
   public boolean isSuccessful() {
      return false;
   }
}
