package org.infinispan.container.versioning;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.marshall.core.Ids;

/**
 * Numeric version
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class NumericVersion implements IncrementableEntryVersion {

   private final long version;

   public NumericVersion(long version) {
      this.version = version;
   }

   public long getVersion() {
      return version;
   }

   @Override
   public InequalVersionComparisonResult compareTo(EntryVersion other) {
      if (other instanceof NumericVersion) {
         NumericVersion otherVersion = (NumericVersion) other;
         if (version < otherVersion.version)
            return InequalVersionComparisonResult.BEFORE;
         else if (version > otherVersion.version)
            return InequalVersionComparisonResult.AFTER;
         else
            return InequalVersionComparisonResult.EQUAL;
      }

      throw new IllegalArgumentException(
            "Unable to compare other types: " + other.getClass().getName());
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      NumericVersion that = (NumericVersion) o;

      if (version != that.version) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return (int) (version ^ (version >>> 32));
   }

   @Override
   public String toString() {
      return "NumericVersion{" +
            "version=" + version +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<NumericVersion> {

      @Override
      public Set<Class<? extends NumericVersion>> getTypeClasses() {
         return Collections.<Class<? extends NumericVersion>>singleton(NumericVersion.class);
      }

      @Override
      public void writeObject(UserObjectOutput output, NumericVersion object) throws IOException {
         output.writeLong(object.version);
      }

      @Override
      public NumericVersion readObject(UserObjectInput input) throws IOException {
         return new NumericVersion(input.readLong());
      }

      @Override
      public Integer getId() {
         return Ids.NUMERIC_VERSION;
      }

   }

}
