package org.infinispan.objectfilter;

/**
 * A single-method callback that is specified when registering a filter with a Matcher. The {@link #onFilterResult}
 * method is notified of all instances that were presented to {@link Matcher#match} and successfully matched the filter
 * associated with this callback. The callback will receive the instance being matched, the projected fields (optional,
 * if specified) and the 'order by' projections (optional, if specified). The 'order by' projection is an array of
 * {@link java.lang.Comparable} that can be compared using the {@link java.util.Comparator} provided by {@link
 * FilterSubscription#getComparator()}.
 * <p/>
 * Implementations of this interface are provided by the subscriber and must written is such a way that they can be
 * invoked from multiple threads simultaneously.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface FilterCallback {

   /**
    * Receives notification that an instance matches the filter.
    *
    * @param instance       the object being matched
    * @param projection     the projection, if a projection was requested or {@code null} otherwise
    * @param sortProjection the projection of fields used for sorting, if sorting was requested or {@code null}
    *                       otherwise
    */
   void onFilterResult(Object instance, Object[] projection, Comparable[] sortProjection);
}
