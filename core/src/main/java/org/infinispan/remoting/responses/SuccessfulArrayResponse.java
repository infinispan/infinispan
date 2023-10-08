package org.infinispan.remoting.responses;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SUCCESSFUL_ARRAY_RESPONSE)
public class SuccessfulArrayResponse<T> implements SuccessfulResponse<Object[]> {

   @ProtoField(1)
   final MarshallableArray<T> array;

   @ProtoFactory
   static <T> SuccessfulArrayResponse<T> protoFactory(MarshallableArray<T> array) {
      return array == null ? null : new SuccessfulArrayResponse<>(array);
   }

   @SuppressWarnings("unchecked")
   SuccessfulArrayResponse(Object[] array) {
      this.array = MarshallableArray.create((T[]) array);
   }

   SuccessfulArrayResponse(MarshallableArray<T> array) {
      this.array = array;
   }

   public Object[] getResponseValue() {
      return array.get();
   }

   public T[] toArray(T[] array) {
      return MarshallableArray.unwrap(this.array, array);
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      SuccessfulArrayResponse<T> that = (SuccessfulArrayResponse<T>) o;
      return Objects.equals(array, that.array);
   }

   @Override
   public int hashCode() {
      return Objects.hashCode(array);
   }

   @Override
   public String toString() {
      return "SuccessfulArrayResponse{" +
            "array=" + array +
            '}';
   }
}
