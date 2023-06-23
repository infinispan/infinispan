package org.infinispan.multimap.impl.internal;

import org.infinispan.commons.util.Util;
import org.infinispan.marshall.protostream.impl.MarshallableUserObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

import java.util.Arrays;
import java.util.Objects;

import static org.infinispan.commons.marshall.ProtoStreamTypeIds.MULTIMAP_OBJECT_WRAPPER;

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
      if (!(obj instanceof MultimapObjectWrapper)) return false;

      MultimapObjectWrapper<?> other = (MultimapObjectWrapper<?>) obj;
      if (object instanceof byte[] && other.object instanceof byte[])
         return Arrays.equals((byte[]) object, (byte[]) other.object);

      return Objects.equals(object, other.object);
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
