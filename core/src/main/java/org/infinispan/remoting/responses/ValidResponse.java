package org.infinispan.remoting.responses;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * A valid response
 *
 * @author manik
 * @since 4.0
 */
public abstract class ValidResponse implements Response {

   @ProtoField(1)
   protected MarshallableObject<?> object;

   @ProtoField(2)
   protected MarshallableCollection<?> collection;

   @ProtoField(3)
   protected MarshallableMap<?, ?> map;

   @ProtoField(4)
   protected MarshallableArray<?> array;

   @ProtoField(5)
   protected byte[] bytes;

   protected ValidResponse() {
      this(null, null, null, null, null);
   }

   protected ValidResponse(MarshallableObject<?> object, MarshallableCollection<?> collection,
                           MarshallableMap<?, ?> map, MarshallableArray<?> array, byte[] bytes) {
      this.object = object;
      this.collection = collection;
      this.map = map;
      this.array = array;
      this.bytes = bytes;
   }

   /**
    * @return the underlying object contained in the response which could be a normal Object, Collection, Map or Array.
    * This method should only be utilised when the response objects type is not known, i.e. when the value must be passed
    * on to another component.
    */
   public Object getResponseValue() {
      isReturnValueDisabledCheck();
      if (object != null)
         return object.get();

      if (collection != null)
         return collection.get();

      if (map != null)
         return map.get();

      if (array != null)
         return array.get();

      return bytes;
   }

   /**
    * @return the response object cast to the required type, or null if no value exists. Use this method when the response type is known.
    */
   @SuppressWarnings("unchecked")
   public <T> T getResponseObject() {
      isReturnValueDisabledCheck();
      return MarshallableObject.unwrap((MarshallableObject<T>) object);
   }

   /**
    * @return the response {@link Collection} cast to the required type, or null if no value exists.
    * Use this method when the response must return a {@link Collection}.
    */
   @SuppressWarnings("unchecked")
   public <T> Collection<T> getResponseCollection() {
      isReturnValueDisabledCheck();
      return MarshallableCollection.unwrap((MarshallableCollection<T>) collection);
   }

   @SuppressWarnings("unchecked")
   public <T> List<T> getResponseList() {
      isReturnValueDisabledCheck();
      return MarshallableCollection.unwrap((MarshallableCollection<T>) collection, ArrayList::new);
   }

   /**
    * @param a the array into which elements will be stored.
    * @return an array containing the response elements
    */
   @SuppressWarnings("unchecked")
   public <T> T[] getResponseArray(T[] a) {
      isReturnValueDisabledCheck();
      return MarshallableArray.unwrap((MarshallableArray<T>) array, a);
   }

   /**
    * @return the response map with the required casting.
    */
   @SuppressWarnings("unchecked")
   public <K, V> Map<K, V> getResponseMap() {
      isReturnValueDisabledCheck();
      return MarshallableMap.unwrap((MarshallableMap<K, V>) map);
   }

   @Override
   public boolean isValid() {
      return true;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ValidResponse that = (ValidResponse) o;
      return Objects.equals(object, that.object) &&
            Objects.equals(collection, that.collection) &&
            Objects.equals(map, that.map) &&
            Objects.equals(array, that.array);
   }

   @Override
   public int hashCode() {
      return Objects.hash(object, collection, map, array);
   }

   @Override
   public String toString() {
      return "ValidResponse{" +
            "object=" + object +
            ", collection=" + collection +
            ", map=" + map +
            ", array=" + array +
            '}';
   }

   protected void isReturnValueDisabledCheck() {
      if (isReturnValueDisabled())
         throw new UnsupportedOperationException();
   }

   protected boolean isReturnValueDisabled() {
      return false;
   }
}
