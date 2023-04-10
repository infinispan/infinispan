package org.infinispan.server.memcached;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Memcached metadata information.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@ProtoTypeId(ProtoStreamTypeIds.MEMCACHED_METADATA)
public class MemcachedMetadata extends EmbeddedMetadata.EmbeddedLifespanExpirableMetadata {

   @ProtoField(number = 5, defaultValue = "0")
   public final int flags;

   @ProtoFactory
   public MemcachedMetadata(int flags, long lifespan, NumericVersion numericVersion, SimpleClusteredVersion clusteredVersion) {
      this(flags, lifespan, numericVersion != null ? numericVersion : clusteredVersion);
   }

   private MemcachedMetadata(int flags, long lifespan, EntryVersion version) {
      super(lifespan, version);
      this.flags = flags;
   }

   @Override
   public boolean isEmpty() {
      return super.isEmpty() && flags == 0;
   }

   @Override
   public Metadata.Builder builder() {
      return new Builder()
            .flags(flags)
            .lifespan(lifespan())
            .version(version());
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      MemcachedMetadata that = (MemcachedMetadata) o;
      return flags == that.flags;
   }

   @Override
   public int hashCode() {
      return Objects.hash(super.hashCode(), flags);
   }

   @Override
   public String toString() {
      return "MemcachedMetadata{" +
            "flags=" + flags +
            ", version=" + version() +
            ", lifespan=" + lifespan() +
            '}';
   }

   public static class Builder extends EmbeddedMetadata.Builder {

      private int flags;

      public Builder flags(int flags) {
         this.flags = flags;
         return this;
      }

      @Override
      public Metadata build() {
         return new MemcachedMetadata(flags, lifespan == null ? -1 : lifespanUnit.toMillis(lifespan), version);
      }
   }
}
