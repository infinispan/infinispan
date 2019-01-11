package org.infinispan.query.remote.client;

import java.util.Arrays;

/**
 * When using Ickle based filters with client event listeners you will get the event data (see
 * org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent.getEventData) wrapped by this FilterResult.
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
public final class FilterResult {

   private final Object instance;

   private final Object[] projection;

   private final Comparable[] sortProjection;

   public FilterResult(Object instance, Object[] projection, Comparable[] sortProjection) {
      if (instance == null && projection == null) {
         throw new IllegalArgumentException("instance and projection cannot be both null");
      }
      this.instance = instance;
      this.projection = projection;
      this.sortProjection = sortProjection;
   }

   /**
    * Returns the matched object. This is non-null unless projections are present.
    */
   public Object getInstance() {
      return instance;
   }

   /**
    * Returns the projection, if a projection was requested or {@code null} otherwise.
    */
   public Object[] getProjection() {
      return projection;
   }

   /**
    * Returns the projection of fields that appear in the 'order by' clause, if any, or {@code null} otherwise.
    * <p>
    * Please note that no actual sorting is performed! The 'order by' clause is ignored but the fields listed there are
    * still projected and returned so the caller can easily sort the results if needed. Do not use 'order by' with
    * filters if this behaviour does not suit you.
    */
   public Comparable[] getSortProjection() {
      return sortProjection;
   }

   @Override
   public String toString() {
      return "FilterResult{" +
            "instance=" + instance +
            ", projection=" + Arrays.toString(projection) +
            ", sortProjection=" + Arrays.toString(sortProjection) +
            '}';
   }
}
