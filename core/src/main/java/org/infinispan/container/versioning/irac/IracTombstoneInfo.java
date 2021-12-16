package org.infinispan.container.versioning.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

import org.infinispan.commons.util.Util;
import org.infinispan.metadata.impl.IracMetadata;

/**
 * A data class to store the tombstone information for a key.
 *
 * @since 14.0
 */
public class IracTombstoneInfo {

   private final Object key;
   private final int segment;
   private final IracMetadata metadata;

   public IracTombstoneInfo(Object key, int segment, IracMetadata metadata) {
      this.key = Objects.requireNonNull(key);
      this.segment = segment;
      this.metadata = Objects.requireNonNull(metadata);
   }

   public Object getKey() {
      return key;
   }

   public int getSegment() {
      return segment;
   }

   public IracMetadata getMetadata() {
      return metadata;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      IracTombstoneInfo that = (IracTombstoneInfo) o;

      if (segment != that.segment) return false;
      if (!key.equals(that.key)) return false;
      return metadata.equals(that.metadata);
   }

   @Override
   public int hashCode() {
      int result = key.hashCode();
      result = 31 * result + segment;
      result = 31 * result + metadata.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "IracTombstoneInfo{" +
            "key=" + Util.toStr(key) +
            ", segment=" + segment +
            ", metadata=" + metadata +
            '}';
   }

   public static void writeTo(ObjectOutput output, IracTombstoneInfo tombstone) throws IOException {
      if (tombstone == null) {
         output.writeObject(null);
         return;
      }
      output.writeObject(tombstone.key);
      output.writeInt(tombstone.segment);
      IracMetadata.writeTo(output, tombstone.metadata);
   }

   public static IracTombstoneInfo readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      Object key = input.readObject();
      return key == null ? null : new IracTombstoneInfo(key, input.readInt(), IracMetadata.readFrom(input));
   }
}
