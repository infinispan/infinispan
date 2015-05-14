package org.infinispan.container.versioning;

/**
 * Versions can be compared to each other to result in one version being before, after or at the same time as another
 * version.  This is different from the JDK's {@link Comparable} interface, which is much more simplistic in that it
 * doesn't differentiate between something that is the same versus equal-but-different.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public enum InequalVersionComparisonResult {
   /**
    * Denotes a version that was created temporally <i>before</i> another version.
    */
   BEFORE,
   /**
    * Denotes a version that was created temporally <i>after</i> another version.
    */
   AFTER,
   /**
    * Denotes that the two versions being compared are equal.
    */
   EQUAL,
   /**
    * Denotes a version that was created at the same time as another version, but is not equal.  This is only really
    * useful when using a partition-aware versioning scheme, such as vector or Lamport clocks.
    */
   CONFLICTING
}
