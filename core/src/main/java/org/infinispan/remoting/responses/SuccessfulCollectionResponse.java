package org.infinispan.remoting.responses;

import java.util.Collection;
import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SUCCESSFUL_COLLECTION_RESPONSE)
public class SuccessfulCollectionResponse<T> implements SuccessfulResponse<Collection<T>> {

   @ProtoField(1)
   final MarshallableCollection<T> collection;

   @ProtoFactory
   static <T> SuccessfulCollectionResponse<T> protoFactory(MarshallableCollection<T> collection) {
      return collection == null ? null : new SuccessfulCollectionResponse<>(collection);
   }

   SuccessfulCollectionResponse(Collection<T> collection) {
      this.collection = MarshallableCollection.create(collection);
   }

   SuccessfulCollectionResponse(MarshallableCollection<T> collection) {
      this.collection = collection;
   }

   public Collection<T> getResponseValue() {
      return MarshallableCollection.unwrap(collection);
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      SuccessfulCollectionResponse<?> that = (SuccessfulCollectionResponse<?>) o;
      return Objects.equals(collection, that.collection);
   }

   @Override
   public int hashCode() {
      return Objects.hashCode(collection);
   }

   @Override
   public String toString() {
      return "SuccessfulCollectionResponse{" +
            "collection=" + collection +
            '}';
   }
}
