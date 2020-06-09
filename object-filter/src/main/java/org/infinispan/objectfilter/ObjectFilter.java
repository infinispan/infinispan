package org.infinispan.objectfilter;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 * A filter that tests if an object matches a pre-defined condition and returns either the original instance or the
 * projection, depending on how the filter was created. The projection is represented as an Object[]. If the given
 * instance does not match the filter will just return null.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface ObjectFilter {

   /**
    * The fully qualified entity type name accepted by this filter.
    */
   String getEntityTypeName();

   /**
    * The array of '.' separated path names of the projected fields if any, or {@code null} otherwise.
    */
   String[] getProjection();

   /**
    * The types of the projects paths (see {@link #getProjection()} or {@code null} if there are not projections.
    */
   Class<?>[] getProjectionTypes();

   /**
    * Returns the parameter names or an empty Set if there are no parameters.
    */
   Set<String> getParameterNames();

   /**
    * The parameter values. Please do not mutate this map. Obtain a new filter instance with new parameter values using
    * {@link #withParameters(Map)}.
    */
   Map<String, Object> getParameters();

   /**
    * Creates a new ObjectFilter instance based on the current one and the given parameter values.
    */
   ObjectFilter withParameters(Map<String, Object> namedParameters);

   /**
    * The array of sort specifications if defined, or {@code null} otherwise.
    */
   SortField[] getSortFields();

   /**
    * The comparator corresponding to the 'order by' clause, if any.
    *
    * @return the Comparator or {@code null} if no 'order by' was specified ({@link #getSortFields()} also returns
    * {@code null})
    */
   Comparator<Comparable<?>[]> getComparator();

   /**
    * Tests if an object instance matches the filter.
    *
    * @param instance the instance to test; this is never {@code null}
    * @return a {@code FilterResult} if there is a match or {@code null} otherwise
    */
   FilterResult filter(Object instance);

   /**
    * The output of the {@link ObjectFilter#filter} method.
    */
   interface FilterResult {

      /**
       * Returns the matched object. This is non-null unless projections are present.
       */
      Object getInstance();

      /**
       * Returns the projection, if a projection was requested or {@code null} otherwise.
       */
      Object[] getProjection();

      /**
       * Returns the projection of fields used for sorting, if sorting was requested or {@code null} otherwise.
       */
      Comparable[] getSortProjection();
   }
}
