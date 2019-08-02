package org.infinispan.server.memcached;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * Memcached metadata information.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
class MemcachedMetadata extends EmbeddedMetadata.EmbeddedLifespanExpirableMetadata {

   @ProtoField(number = 5, defaultValue = "-1")
   long flags;

   MemcachedMetadata() {}

   private MemcachedMetadata(long flags, EntryVersion version, long lifespan, TimeUnit lifespanUnit) {
      super(lifespan, lifespanUnit, version);
      this.flags = flags;
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
            ", lifespanTime=" + lifespan() +
            '}';
   }

   static class Builder extends EmbeddedMetadata.Builder {

      private long flags;

      Builder flags(long flags) {
         this.flags = flags;
         return this;
      }

      @Override
      public Metadata build() {
         return new MemcachedMetadata(flags, version, lifespan == null ? -1 : lifespan, lifespanUnit);
      }
   }
}
