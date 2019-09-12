package org.infinispan.metadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
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

   @ProtoField(number = 1)
   public NumericVersion getNumericVersion() {
      return version instanceof NumericVersion ? (NumericVersion) version : null;
   }

   @ProtoField(number = 2)
   public SimpleClusteredVersion getClusteredVersion() {
      return version instanceof SimpleClusteredVersion ? (SimpleClusteredVersion) version : null;
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

   public static class Externalizer extends AbstractExternalizer<EmbeddedMetadata> {

      private static final int IMMORTAL = 0;
      private static final int EXPIRABLE = 1;
      private static final int LIFESPAN_EXPIRABLE = 2;
      private static final int MAXIDLE_EXPIRABLE = 3;
      private final Map<Class<?>, Integer> numbers = new HashMap<>(2);

      public Externalizer() {
         numbers.put(EmbeddedMetadata.class, IMMORTAL);
         numbers.put(EmbeddedExpirableMetadata.class, EXPIRABLE);
         numbers.put(EmbeddedLifespanExpirableMetadata.class, LIFESPAN_EXPIRABLE);
         numbers.put(EmbeddedMaxIdleExpirableMetadata.class, MAXIDLE_EXPIRABLE);
      }

      @Override
      public Set<Class<? extends EmbeddedMetadata>> getTypeClasses() {
         return Util.asSet(EmbeddedMetadata.class, EmbeddedExpirableMetadata.class,
               EmbeddedLifespanExpirableMetadata.class, EmbeddedMaxIdleExpirableMetadata.class);
      }

      @Override
      public Integer getId() {
         return Ids.EMBEDDED_METADATA;
      }

      @Override
      public void writeObject(ObjectOutput output, EmbeddedMetadata object) throws IOException {
         int number = numbers.getOrDefault(object.getClass(), -1);
         output.write(number);
         switch (number) {
            case EXPIRABLE:
               output.writeLong(object.lifespan());
               output.writeLong(object.maxIdle());
               break;
            case LIFESPAN_EXPIRABLE:
               output.writeLong(object.lifespan());
               break;
            case MAXIDLE_EXPIRABLE:
               output.writeLong(object.maxIdle());
               break;
         }
         output.writeObject(object.version());
      }

      @Override
      public EmbeddedMetadata readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int number = input.readByte();
         long lifespan;
         long maxIdle;
         switch (number) {
            case IMMORTAL:
               return new EmbeddedMetadata((EntryVersion) input.readObject());
            case EXPIRABLE:
               lifespan = toMillis(input.readLong(), TimeUnit.MILLISECONDS);
               maxIdle = toMillis(input.readLong(), TimeUnit.MILLISECONDS);
               return new EmbeddedExpirableMetadata(lifespan, maxIdle, (EntryVersion) input.readObject());
            case LIFESPAN_EXPIRABLE:
               lifespan = toMillis(input.readLong(), TimeUnit.MILLISECONDS);
               return new EmbeddedLifespanExpirableMetadata(lifespan, (EntryVersion) input.readObject());
            case MAXIDLE_EXPIRABLE:
               maxIdle = toMillis(input.readLong(), TimeUnit.MILLISECONDS);
               return new EmbeddedMaxIdleExpirableMetadata(maxIdle, (EntryVersion) input.readObject());
            default:
               throw new IllegalStateException("Unknown metadata type " + number);
         }
      }
   }

   private static long toMillis(long duration, TimeUnit timeUnit) {
      return duration < 0 ? -1 : timeUnit.toMillis(duration);
   }
}
