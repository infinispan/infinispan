package org.infinispan.query.objectfilter;

import org.infinispan.query.objectfilter.impl.ql.PropertyPath;

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
   PropertyPath<?> getPath();

   /**
    * Indicates if sorting is ascending or descending.
    */
   boolean isAscending();
}
