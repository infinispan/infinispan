package org.infinispan.objectfilter;

/**
 * A filter that tests if an object matches a pre-defined condition and returns either the original instance or the
 * projection, depending on how the filter was created. The projection is represented as an Object[]. If the given
 * instance does not match the filter will return null.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface ObjectFilter {

   String[] getProjection();

   /**
    * Tests if an object instance matches the filter.
    *
    * @param instance the instance to test; this is never null
    * @return a non-null value if there is a match or null otherwise; the returned value is initial object instance  or
    * its projection in the form of an Object[] if one was specified
    */
   Object filter(Object instance);
}
