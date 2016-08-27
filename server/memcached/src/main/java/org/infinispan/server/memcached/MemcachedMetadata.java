package org.infinispan.server.memcached;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;

/**
 * Memcached metadata information.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class MemcachedMetadata implements Metadata {
   protected final long flags;
   protected final EntryVersion version;

   public MemcachedMetadata(long flags, EntryVersion version) {
      this.flags = flags;
      this.version = Objects.requireNonNull(version);
   }

   @Override
   public long lifespan() {
      return -1;
   }

   @Override
   public long maxIdle() {
      return -1;
   }

   @Override
   public EntryVersion version() {
      return version;
   }

   @Override
   public Builder builder() {
      return new MemcachedMetadataBuilder().flags(flags).version(version);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MemcachedMetadata that = (MemcachedMetadata) o;

      if (flags != that.flags) return false;
      return version.equals(that.version);

   }

   @Override
   public int hashCode() {
      int result = (int) (flags ^ (flags >>> 32));
      result = 31 * result + version.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "MemcachedMetadata{" +
              "flags=" + flags +
              ", version=" + version +
              '}';
   }
}

class MemcachedExpirableMetadata extends MemcachedMetadata {

   protected final long lifespanTime;
   protected final TimeUnit lifespanUnit;

   MemcachedExpirableMetadata(long flags, EntryVersion version, long lifespanTime, TimeUnit lifespanUnit) {
      super(flags, version);
      this.lifespanTime = lifespanTime;
      this.lifespanUnit = Objects.requireNonNull(lifespanUnit);
   }

   @Override
   public long lifespan() {
      return lifespanUnit.toMillis(lifespanTime);
   }

   @Override
   public Builder builder() {
      return new MemcachedMetadataBuilder().flags(flags).version(version).lifespan(lifespanTime, lifespanUnit);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      MemcachedExpirableMetadata that = (MemcachedExpirableMetadata) o;

      if (lifespanTime != that.lifespanTime) return false;
      return lifespanUnit == that.lifespanUnit;

   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (lifespanTime ^ (lifespanTime >>> 32));
      result = 31 * result + lifespanUnit.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "MemcachedExpirableMetadata{" +
              "flags=" + flags +
              ", version=" + version +
              ", lifespanTime=" + lifespanTime +
              ", lifespanUnit=" + lifespanUnit +
              "} ";
   }

}

class MemcachedMetadataBuilder extends EmbeddedMetadata.Builder {

   private long flags;

   MemcachedMetadataBuilder flags(long flags) {
      this.flags = flags;
      return this;
   }

   @Override
   public Metadata build() {
      if (hasLifespan())
         return new MemcachedExpirableMetadata(flags, version, lifespan, lifespanUnit);
      else
         return new MemcachedMetadata(flags, version);
   }
}
