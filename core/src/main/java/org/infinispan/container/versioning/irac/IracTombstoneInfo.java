package org.infinispan.container.versioning.irac;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A data class to store the tombstone information for a key.
 *
 * @since 14.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_TOMBSTONE_INFO)
public class IracTombstoneInfo {

   private final Object key;
   private final int segment;
   private final IracMetadata metadata;

   public IracTombstoneInfo(Object key, int segment, IracMetadata metadata) {
      this.key = Objects.requireNonNull(key);
      this.segment = segment;
      this.metadata = Objects.requireNonNull(metadata);
   }

   @ProtoFactory
   IracTombstoneInfo(MarshallableObject<Object> wrappedKey, int segment, IracMetadata metadata) {
      this(MarshallableObject.unwrap(wrappedKey), segment, metadata);
   }

   @ProtoField(number = 1)
   MarshallableObject<Object> getWrappedKey() {
      return MarshallableObject.create(key);
   }

   public Object getKey() {
      return key;
   }

   @ProtoField(number = 2, defaultValue = "-1")
   public int getSegment() {
      return segment;
   }

   @ProtoField(number = 3)
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
}
