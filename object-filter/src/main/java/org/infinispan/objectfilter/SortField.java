package org.infinispan.objectfilter;

/**
 * Sort specification for a field.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface SortField {

   /**
    * The field path.
    */
   PropertyPath getPath();

   /**
    * Indicates if sorting is ascending or descending.
    */
   boolean isAscending();
}
