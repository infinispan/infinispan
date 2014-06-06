package org.infinispan.objectfilter;

import java.util.Comparator;

/**
 * A subscription for match notifications.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface FilterSubscription {

   /**
    * The fully qualified entity type name accepted by this filter.
    */
   String getEntityTypeName();

   /**
    * The associated callback that is being notified of successful matches.
    */
   FilterCallback getCallback();

   /**
    * The array of '.' separated path names of the projected fields if any, or {@code null} otherwise.
    */
   String[] getProjection();

   /**
    * The array of sort specifications if defined, or {@code null} otherwise.
    */
   SortField[] getSortFields();

   /**
    * The comparator corresponding to the 'order by' clause, if any.
    *
    * @return the Comparator or {@code null} if no 'order by' was specified
    */
   Comparator<Comparable[]> getComparator();
}
