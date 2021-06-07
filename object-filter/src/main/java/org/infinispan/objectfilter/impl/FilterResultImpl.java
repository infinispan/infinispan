package org.infinispan.objectfilter.impl;

import java.util.Arrays;

import org.infinispan.objectfilter.ObjectFilter;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterResultImpl implements ObjectFilter.FilterResult {

   private final Object key;

   private final Object instance;

   private final Object[] projection;

   private final Comparable[] sortProjection;

   public FilterResultImpl(Object key, Object instance, Object[] projection, Comparable[] sortProjection) {
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
