package org.infinispan.functional;

// TODO: Entry version defined in core/ and does not expose internal entry

import org.infinispan.commons.util.Experimental;

/**
 * Entry version.
 *
 * @since 8.0
 */
@Experimental
public interface EntryVersion<T> {

   /**
    * Get the underlying representation of the entry's version.
    */
   T get();

   /**
    * Compare the entry version.
    */
   CompareResult compareTo(EntryVersion<T> other);

   enum CompareResult {
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

   final class NumericEntryVersion implements EntryVersion<Long> {
      private final long version;

      public NumericEntryVersion(long version) {
         this.version = version;
      }

      @Override
      public Long get() {
         return version;
      }

      @Override
      public CompareResult compareTo(EntryVersion<Long> other) {
         if (version < other.get()) return CompareResult.BEFORE;
         else if (version > other.get()) return CompareResult.AFTER;
         else return CompareResult.EQUAL;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         NumericEntryVersion that = (NumericEntryVersion) o;

         return version == that.version;
      }

      @Override
      public int hashCode() {
         return (int) (version ^ (version >>> 32));
      }

      @Override
      public String toString() {
         return "NumericEntryVersion=" + version;
      }
   }
}
