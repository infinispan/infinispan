package org.infinispan.container.versioning;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.marshall.core.Ids;
import org.infinispan.protostream.MessageMarshaller;

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
      public void writeObject(ObjectOutput output, NumericVersion object) throws IOException {
         output.writeLong(object.version);
      }

      @Override
      public NumericVersion readObject(ObjectInput input) throws IOException {
         return new NumericVersion(input.readLong());
      }

      @Override
      public Integer getId() {
         return Ids.NUMERIC_VERSION;
      }

   }

   public static class Marshaller implements MessageMarshaller<NumericVersion> {
      @Override
      public NumericVersion readFrom(ProtoStreamReader reader) throws IOException {
         return new NumericVersion(reader.readLong("version"));
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, NumericVersion numericVersion) throws IOException {
         writer.writeLong("version", numericVersion.version);
      }

      @Override
      public Class<NumericVersion> getJavaClass() {
         return NumericVersion.class;
      }

      @Override
      public String getTypeName() {
         return "persistence.NumericVersion";
      }
   }
}
