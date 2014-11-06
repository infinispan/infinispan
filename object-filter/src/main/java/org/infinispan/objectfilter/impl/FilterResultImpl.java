package org.infinispan.objectfilter.impl;

import org.infinispan.objectfilter.ObjectFilter;

import java.util.Arrays;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterResultImpl implements ObjectFilter.FilterResult {

   private final Object instance;

   private final Object[] projection;

   private final Comparable[] sortProjection;

   public FilterResultImpl(Object instance, Object[] projection, Comparable[] sortProjection) {
      if (instance == null && projection == null) {
         throw new IllegalArgumentException("instance and projection cannot be both null");
      }
      this.instance = instance;
      this.projection = projection;
      this.sortProjection = sortProjection;
   }

   public Object getInstance() {
      return instance;
   }

   public Object[] getProjection() {
      return projection;
   }

   public Comparable[] getSortProjection() {
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
