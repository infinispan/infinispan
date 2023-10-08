package org.infinispan.objectfilter.impl;

import java.util.Arrays;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@ProtoTypeId(ProtoStreamTypeIds.ICKLE_FILTER_RESULT)
public final class FilterResultImpl implements ObjectFilter.FilterResult {

   private final Object key;

   private final Object instance;

   private final Object[] projection;

   private final Comparable<?>[] sortProjection;

   public FilterResultImpl(Object key, Object instance, Object[] projection, Comparable<?>[] sortProjection) {
      if (instance != null && projection != null) {
         throw new IllegalArgumentException("instance and projection cannot be both non-null");
      }
      if (instance == null && projection == null) {
         throw new IllegalArgumentException("instance and projection cannot be both null");
      }
      this.key = key;
      this.instance = instance;
      this.projection = projection;
      this.sortProjection = sortProjection;
   }

   @ProtoFactory
   FilterResultImpl(MarshallableObject<Object> wrappedKey, MarshallableArray<Object> wrappedProjection,
                    MarshallableObject<Object> wrappedInstance, MarshallableArray<Comparable<?>> wrappedSortProjection) {
      this(
            MarshallableObject.unwrap(wrappedKey),
            MarshallableObject.unwrap(wrappedInstance),
            MarshallableArray.unwrap(wrappedProjection),
            MarshallableArray.unwrap(wrappedSortProjection, new Comparable[0])
      );
   }

   @ProtoField(number = 1, name = "key")
   MarshallableObject<?> getWrappedKey() {
      return projection != null ? null : MarshallableObject.create(key);
   }

   @ProtoField(number = 2, name = "projection")
   MarshallableArray<?> getWrappedProjection() {
      return MarshallableArray.create(projection);
   }

   @ProtoField(number = 3, name = "instance")
   MarshallableObject<?> getWrappedInstance() {
      return projection != null ? null : MarshallableObject.create(instance);
   }

   @ProtoField(number = 4, name = "sortProjection")
   MarshallableArray<?> getWrappedSortProjection() {
      return MarshallableArray.create(sortProjection);
   }

   @Override
   public Object getKey() {
      return key;
   }

   @Override
   public Object getInstance() {
      return instance;
   }

   @Override
   public Object[] getProjection() {
      return projection;
   }

   @Override
   public Comparable<?>[] getSortProjection() {
      return sortProjection;
   }

   @Override
   public String toString() {
      return "FilterResultImpl{" +
            "instance=" + instance +
            ", projection=" + Arrays.toString(projection) +
            ", sortProjection=" + Arrays.toString(sortProjection) +
            '}';
   }
}
