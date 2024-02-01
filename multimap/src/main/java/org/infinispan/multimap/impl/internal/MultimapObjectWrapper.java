package org.infinispan.multimap.impl.internal;

import static org.infinispan.commons.marshall.ProtoStreamTypeIds.MULTIMAP_OBJECT_WRAPPER;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Objects;

import org.infinispan.commons.util.Util;
import org.infinispan.marshall.protostream.impl.MarshallableUserObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Wrapper for objects stored in multimap buckets.
 * <p>
 * This wrapper provides an implementation for {@link #equals(Object)} and {@link #hashCode()} methods to the underlying
 * object. Making the wrapper fit to store elements in HashMaps or Sets.
 *
 * @param <T>: Type of the underlying object.
 * @since 15.0
 */
@ProtoTypeId(MULTIMAP_OBJECT_WRAPPER)
public class MultimapObjectWrapper<T> implements Comparable<MultimapObjectWrapper> {

   final T object;

   public MultimapObjectWrapper(T object) {
      this.object = object;
   }

   @ProtoFactory
   MultimapObjectWrapper(MarshallableUserObject<T> wrapper) {
      this(wrapper.get());
   }

   public T get() {
      return object;
   }

   @ProtoField(number = 1)
   MarshallableUserObject<T> wrapper() {
      return new MarshallableUserObject<>(object);
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof MultimapObjectWrapper<?> other)) return false;

      if (object instanceof byte[] && other.object instanceof byte[])
         return Arrays.equals((byte[]) object, (byte[]) other.object);

      return Objects.equals(object, other.object);
   }

   public Double asDouble() {
      if (object instanceof byte[]) {
         return Double.valueOf(new String((byte[]) object, Charset.forName("US-ASCII")));
      }
      if (object instanceof Double)
         return (Double) object;

      throw new NumberFormatException("Can't convert to Double from class " + object.getClass());
   }

   @Override
   public int hashCode() {
      if (object instanceof byte[])
         return java.util.Arrays.hashCode((byte[]) object);

      return Objects.hashCode(object);
   }

   @Override
   public int compareTo(MultimapObjectWrapper other) {
      if (other == null) {
         throw new NullPointerException();
      }
      if (this.equals(other)) {
         return 0;
      }

      if (this.object instanceof Comparable && other.object instanceof Comparable) {
         return ((Comparable) this.object).compareTo(other.object);
      }

      if (object instanceof byte[] && other.object instanceof byte[]) {
         return Arrays.compare((byte[]) object, (byte[]) other.object);
      }

      throw new ClassCastException("can't compare");
   }

   @Override
   public String toString() {
      if (object instanceof byte[]) {
         return "MultimapObjectWrapper{" + "object=" + Util.hexDump((byte[])object) + '}';
      }
      return "MultimapObjectWrapper{" + "object=" + object + '}';
   }
}
