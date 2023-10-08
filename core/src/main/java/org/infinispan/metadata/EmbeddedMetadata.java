package org.infinispan.metadata;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Metadata class for embedded caches.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@ProtoTypeId(ProtoStreamTypeIds.EMBEDDED_METADATA)
public class EmbeddedMetadata implements Metadata {
   public static final EmbeddedMetadata EMPTY = new EmbeddedMetadata(null, null);

   protected final EntryVersion version;

   private EmbeddedMetadata(EntryVersion version) {
      this.version = version;
   }

   @ProtoFactory
   EmbeddedMetadata(NumericVersion numericVersion, SimpleClusteredVersion clusteredVersion) {
      version = numericVersion != null ? numericVersion : clusteredVersion;
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

   @ProtoField(1)
   public NumericVersion getNumericVersion() {
      return version instanceof NumericVersion ? (NumericVersion) version : null;
   }

   @ProtoField(2)
   public SimpleClusteredVersion getClusteredVersion() {
      return version instanceof SimpleClusteredVersion ? (SimpleClusteredVersion) version : null;
   }

   @Override
   public boolean isEmpty() {
      return version == null;
   }

   @Override
   public Metadata.Builder builder() {
      return new Builder().version(version);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      EmbeddedMetadata that = (EmbeddedMetadata) o;
      return version != null ? version.equals(that.version) : that.version == null;
   }

   @Override
   public int hashCode() {
      return version != null ? version.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "EmbeddedMetadata{version=" + version + '}';
   }

   public static class Builder implements Metadata.Builder {

      protected Long lifespan = null;
      protected TimeUnit lifespanUnit = TimeUnit.MILLISECONDS;
      protected Long maxIdle = null;
      protected TimeUnit maxIdleUnit = TimeUnit.MILLISECONDS;
      protected EntryVersion version;

      @Override
      public Metadata.Builder lifespan(long time, TimeUnit unit) {
         lifespan = time;
         lifespanUnit = unit;
         return this;
      }

      @Override
      public Metadata.Builder lifespan(long time) {
         return lifespan(time, TimeUnit.MILLISECONDS);
      }

      @Override
      public Metadata.Builder maxIdle(long time, TimeUnit unit) {
         maxIdle = time;
         maxIdleUnit = unit;
         return this;
      }

      @Override
      public Metadata.Builder maxIdle(long time) {
         return maxIdle(time, TimeUnit.MILLISECONDS);
      }

      @Override
      public Metadata.Builder version(EntryVersion version) {
         this.version = version;
         return this;
      }

      @Override
      public Metadata build() {
         boolean hasLifespan = hasLifespan();
         boolean hasMaxIdle = hasMaxIdle();
         if (hasLifespan && hasMaxIdle)
            return new EmbeddedExpirableMetadata(toMillis(lifespan, lifespanUnit), toMillis(maxIdle, maxIdleUnit), version);
         else if (hasLifespan)
            return new EmbeddedLifespanExpirableMetadata(toMillis(lifespan, lifespanUnit), version);
         else if (hasMaxIdle)
            return new EmbeddedMaxIdleExpirableMetadata(toMillis(maxIdle, lifespanUnit), version);
         else
            return new EmbeddedMetadata(version);
      }

      protected boolean hasLifespan() {
         return lifespan != null;
      }

      protected boolean hasMaxIdle() {
         return maxIdle != null;
      }

      @Override
      public Metadata.Builder merge(Metadata metadata) {
         if (lifespan == null) { // if lifespan not set, apply default
            lifespan = metadata.lifespan();
            lifespanUnit = TimeUnit.MILLISECONDS;
         }

         if (maxIdle == null) { // if maxIdle not set, apply default
            maxIdle = metadata.maxIdle();
            maxIdleUnit = TimeUnit.MILLISECONDS;
         }

         if (version == null)
            version = metadata.version();

         return this;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.EMBEDDED_EXPIRABLE_METADATA)
   public static class EmbeddedExpirableMetadata extends EmbeddedMetadata {

      private final long lifespan;

      private final long maxIdle;

      @ProtoFactory
      EmbeddedExpirableMetadata(long lifespan, long maxIdle, NumericVersion numericVersion,
                                SimpleClusteredVersion clusteredVersion) {
         this(lifespan, maxIdle, numericVersion != null ? numericVersion : clusteredVersion);
      }

      private EmbeddedExpirableMetadata(long lifespan, long maxIdle, EntryVersion version) {
         super(version);
         this.lifespan = lifespan;
         this.maxIdle = maxIdle;
      }

      @ProtoField(number = 3, defaultValue = "-1")
      @Override
      public long lifespan() {
         return lifespan;
      }

      @ProtoField(number = 4, defaultValue = "-1")
      @Override
      public long maxIdle() {
         return maxIdle;
      }

      @Override
      public Metadata.Builder builder() {
         return super.builder().lifespan(lifespan).maxIdle(maxIdle);
      }

      @Override
      public boolean isEmpty() {
         return super.isEmpty() && lifespan < 0 && maxIdle < 0;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         if (!super.equals(o)) return false;
         EmbeddedExpirableMetadata that = (EmbeddedExpirableMetadata) o;
         return lifespan == that.lifespan &&
               maxIdle == that.maxIdle;
      }

      @Override
      public int hashCode() {
         return Objects.hash(super.hashCode(), lifespan, maxIdle);
      }

      @Override
      public String toString() {
         return "EmbeddedExpirableMetadata{" +
               "version=" + version +
               ", lifespan=" + lifespan +
               ", maxIdle=" + maxIdle +
               '}';
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.EMBEDDED_LIFESPAN_METADATA)
   public static class EmbeddedLifespanExpirableMetadata extends EmbeddedMetadata {

      private final long lifespan;

      protected EmbeddedLifespanExpirableMetadata(long lifespan, EntryVersion version) {
         super(version);
         this.lifespan = lifespan;
      }

      @ProtoFactory
      protected EmbeddedLifespanExpirableMetadata(long lifespan, NumericVersion numericVersion, SimpleClusteredVersion clusteredVersion) {
         this(lifespan, numericVersion != null ? numericVersion : clusteredVersion);
      }

      @ProtoField(number = 3, defaultValue = "-1")
      @Override
      public long lifespan() {
         return lifespan;
      }

      @Override
      public Metadata.Builder builder() {
         return super.builder().lifespan(lifespan);
      }

      @Override
      public boolean isEmpty() {
         return super.isEmpty() && lifespan < 0;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         if (!super.equals(o)) return false;
         EmbeddedLifespanExpirableMetadata that = (EmbeddedLifespanExpirableMetadata) o;
         return lifespan == that.lifespan;
      }

      @Override
      public int hashCode() {
         return Objects.hash(super.hashCode(), lifespan);
      }

      @Override
      public String toString() {
         return "EmbeddedLifespanExpirableMetadata{" +
               "lifespan=" + lifespan +
               ", version=" + version +
               '}';
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.EMBEDDED_MAX_IDLE_METADATA)
   public static class EmbeddedMaxIdleExpirableMetadata extends EmbeddedMetadata {

      private final long maxIdle;

      @ProtoFactory
      EmbeddedMaxIdleExpirableMetadata(long maxIdle, NumericVersion numericVersion, SimpleClusteredVersion clusteredVersion) {
         this(maxIdle, numericVersion != null ? numericVersion : clusteredVersion);
      }

      private EmbeddedMaxIdleExpirableMetadata(long maxIdle, EntryVersion entryVersion) {
         super(entryVersion);
         this.maxIdle = maxIdle;
      }

      @ProtoField(number = 3, defaultValue = "-1")
      @Override
      public long maxIdle() {
         return maxIdle;
      }

      @Override
      public boolean isEmpty() {
         return super.isEmpty() && maxIdle < 0;
      }

      @Override
      public Metadata.Builder builder() {
         return super.builder().maxIdle(maxIdle);
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         if (!super.equals(o)) return false;
         EmbeddedMaxIdleExpirableMetadata that = (EmbeddedMaxIdleExpirableMetadata) o;
         return maxIdle == that.maxIdle;
      }

      @Override
      public int hashCode() {
         return Objects.hash(super.hashCode(), maxIdle);
      }

      @Override
      public String toString() {
         return "EmbeddedMaxIdleExpirableMetadata{" +
               "version=" + version +
               ", maxIdle=" + maxIdle +
               '}';
      }
   }

   private static long toMillis(long duration, TimeUnit timeUnit) {
      return duration < 0 ? -1 : timeUnit.toMillis(duration);
   }
}
