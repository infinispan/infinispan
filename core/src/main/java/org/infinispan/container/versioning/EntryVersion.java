package org.infinispan.container.versioning;

/**
 * A version is used to compare entries against one another.  Versions do not guarantee contiguity, but do guarantee
 * to be comparable.  However this comparability is not the same as the JDK's {@link Comparable} interface.  It is
 * richer in that {@link Comparable} doesn't differentiate between instances that are the same versus instances that
 * are equal-but-different.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public interface EntryVersion {

   /**
    * Compares the given version against the current instance.
    * @param other the other version to compare against
    * @return a InequalVersionComparisonResult instance
    */
   InequalVersionComparisonResult compareTo(EntryVersion other);
}
