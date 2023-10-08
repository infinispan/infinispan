package org.infinispan.remoting.responses;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SUCCESSFUL_OBJECT_RESPONSE)
public class SuccessfulObjResponse<T> implements SuccessfulResponse<T> {
   @ProtoField(1)
   final MarshallableObject<T> object;

   @ProtoFactory
   @SuppressWarnings("unchecked")
   static <T> SuccessfulObjResponse<T> protoFactory(MarshallableObject<T> object) {
      return object == null ? (SuccessfulObjResponse<T>) SUCCESSFUL_EMPTY_RESPONSE : new SuccessfulObjResponse<>(object);
   }

   SuccessfulObjResponse(T object) {
      this(MarshallableObject.create(object));
   }

   SuccessfulObjResponse(MarshallableObject<T> object) {
      this.object = object;
   }

   public T getResponseValue() {
      return MarshallableObject.unwrap(object);
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      SuccessfulObjResponse<?> that = (SuccessfulObjResponse<?>) o;
      return Objects.equals(object, that.object);
   }

   @Override
   public int hashCode() {
      return Objects.hashCode(object);
   }

   @Override
   public String toString() {
      return "SuccessfulObjResponse{" +
            "object=" + object +
            '}';
   }
}
