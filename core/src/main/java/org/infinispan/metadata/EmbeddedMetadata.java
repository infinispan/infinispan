package org.infinispan.metadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationRecord;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.marshall.core.Ids;
import org.jboss.marshalling.util.IdentityIntMap;

/**
 * Metadata class for embedded caches.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class EmbeddedMetadata implements Metadata {

   final EntryVersion version;
   final InvocationRecord records;

   private EmbeddedMetadata(InvocationRecord records, EntryVersion version) {
      this.version = version;
      this.records = records;
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
   public InvocationRecord lastInvocation() {
      return records;
   }

   @Override
   public InvocationRecord invocation(CommandInvocationId id) {
      return InvocationRecord.lookup(records, id);
   }

   @Override
   public Metadata.Builder builder() {
      return new Builder().version(version).invocations(records);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      EmbeddedMetadata that = (EmbeddedMetadata) o;

      if (version != null ? !version.equals(that.version) : that.version != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      return version != null ? version.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "EmbeddedMetadata{" +
            "version=" + version +
            ", records=" + records +
            '}';
   }

   public static class Builder implements Metadata.Builder {

      protected Long lifespan = null;
      protected TimeUnit lifespanUnit = TimeUnit.MILLISECONDS;
      protected Long maxIdle = null;
      protected TimeUnit maxIdleUnit = TimeUnit.MILLISECONDS;
      protected EntryVersion version;
      protected InvocationRecord records;

      public static Metadata.Builder from(Metadata metadata) {
         if (metadata == null) {
            return new EmbeddedMetadata.Builder();
         } else {
            return metadata.builder();
         }
      }

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
      public Metadata.Builder invocation(CommandInvocationId id, Object previousValue, Metadata previousMetadata, long timestamp) {
         records = new InvocationRecord(id, previousValue, previousMetadata, timestamp, records);
         return this;
      }

      @Override
      public Metadata.Builder invocations(InvocationRecord invocations) {
         records = invocations;
         return this;
      }

      @Override
      public InvocationRecord invocations() {
         return records;
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
            return new EmbeddedExpirableMetadata(
                  records, lifespan, lifespanUnit, maxIdle, maxIdleUnit, version);
         else if (hasLifespan)
            return new EmbeddedLifespanExpirableMetadata(records, lifespan, lifespanUnit, version);
         else if (hasMaxIdle)
            return new EmbeddedMaxIdleExpirableMetadata(records, maxIdle, maxIdleUnit, version);
         else
            return new EmbeddedMetadata(records, version);
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

   private static class EmbeddedExpirableMetadata extends EmbeddedMetadata {

      private final long lifespan;
      private final long maxIdle;

      private EmbeddedExpirableMetadata(
            InvocationRecord records, long lifespan, TimeUnit lifespanUnit,
            long maxIdle, TimeUnit maxIdleUnit, EntryVersion version) {
         super(records, version);
         this.lifespan = lifespan < 0 ? -1 : lifespanUnit.toMillis(lifespan);
         this.maxIdle = maxIdle < 0 ? -1 : maxIdleUnit.toMillis(maxIdle);
      }

      @Override
      public long lifespan() {
         return lifespan;
      }

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

         if (lifespan != that.lifespan) return false;
         if (maxIdle != that.maxIdle) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = super.hashCode();
         result = 31 * result + (int) (lifespan ^ (lifespan >>> 32));
         result = 31 * result + (int) (maxIdle ^ (maxIdle >>> 32));
         return result;
      }

      @Override
      public String toString() {
         return "EmbeddedExpirableMetadata{" +
               "lifespan=" + lifespan +
               ", maxIdle=" + maxIdle +
               ", version=" + version +
               ", records=" + records +
               '}';
      }
   }

   private static abstract class AbstractEmbeddedTimeoutMetadata extends EmbeddedMetadata {

      protected final long timeout;

      private AbstractEmbeddedTimeoutMetadata(InvocationRecord records,
            long timeout, TimeUnit timeoutUnit,
            EntryVersion version) {
         super(records, version);
         this.timeout = timeout < 0 ? -1 : timeoutUnit.toMillis(timeout);
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         if (!super.equals(o)) return false;

         AbstractEmbeddedTimeoutMetadata that = (AbstractEmbeddedTimeoutMetadata) o;

         if (timeout != that.timeout) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = super.hashCode();
         result = 31 * result + (int) (timeout ^ (timeout >>> 32));
         return result;
      }
   }

   private static class EmbeddedLifespanExpirableMetadata extends AbstractEmbeddedTimeoutMetadata {

      private EmbeddedLifespanExpirableMetadata(InvocationRecord records, long lifespan, TimeUnit lifespanUnit, EntryVersion version) {
         super(records, lifespan, lifespanUnit, version);
      }

      @Override
      public long lifespan() {
         return timeout;
      }

      @Override
      public Metadata.Builder builder() {
         return super.builder().lifespan(timeout);
      }

      @Override
      public String toString() {
         return "EmbeddedLifespanExpirableMetadata{" +
               "lifespan=" + timeout +
               ", version=" + version +
               ", records=" + records +
               '}';
      }

   }

   private static class EmbeddedMaxIdleExpirableMetadata extends AbstractEmbeddedTimeoutMetadata {

      private EmbeddedMaxIdleExpirableMetadata(InvocationRecord records, long maxIdle, TimeUnit maxIdleUnit, EntryVersion version) {
         super(records, maxIdle, maxIdleUnit, version);
      }

      @Override
      public long maxIdle() {
         return timeout;
      }

      @Override
      public Metadata.Builder builder() {
         return super.builder().maxIdle(timeout);
      }

      @Override
      public String toString() {
         return "EmbeddedMaxIdleExpirableMetadata{" +
               "maxIdle=" + timeout +
               ", version=" + version +
               ", records=" + records +
               '}';
      }

   }

   public static class Externalizer extends AbstractExternalizer<EmbeddedMetadata> {

      private static final int IMMORTAL = 0;
      private static final int EXPIRABLE = 1;
      private static final int LIFESPAN_EXPIRABLE = 2;
      private static final int MAXIDLE_EXPIRABLE = 3;
      // Since we need to keep upgrade compatibility with 9.1 (and preferably older versions as well)
      // we need to be able to deserialize old metedata from cache stores. Therefore we'll tag new written
      // metadata with this mask, and read data without the mask as before.
      private static final int WITH_INVOCATION_RECORDS_MASK = 4;
      private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<Class<?>>(2);

      public Externalizer() {
         numbers.put(EmbeddedMetadata.class, IMMORTAL);
         numbers.put(EmbeddedExpirableMetadata.class, EXPIRABLE);
         numbers.put(EmbeddedLifespanExpirableMetadata.class, LIFESPAN_EXPIRABLE);
         numbers.put(EmbeddedMaxIdleExpirableMetadata.class, MAXIDLE_EXPIRABLE);
      }

      @Override
      @SuppressWarnings("unchecked")
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
         int number = numbers.get(object.getClass(), -1);
         output.write(number | WITH_INVOCATION_RECORDS_MASK);
         InvocationRecord.writeListTo(output, object.records);
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
         int number = input.readUnsignedByte();
         InvocationRecord records = (number & WITH_INVOCATION_RECORDS_MASK) != 0 ? InvocationRecord.readListFrom(input) : null;
         switch (number & ~WITH_INVOCATION_RECORDS_MASK) {
            case IMMORTAL:
               return new EmbeddedMetadata(
                     records, (EntryVersion) input.readObject());
            case EXPIRABLE:
               long lifespan = input.readLong();
               long maxIdle = input.readLong();
               EntryVersion version = (EntryVersion) input.readObject();
               return new EmbeddedExpirableMetadata(
                     records, lifespan, TimeUnit.MILLISECONDS,
                     maxIdle, TimeUnit.MILLISECONDS, version);
            case LIFESPAN_EXPIRABLE:
               return new EmbeddedLifespanExpirableMetadata(
                     records, input.readLong(), TimeUnit.MILLISECONDS,
                     (EntryVersion) input.readObject());
            case MAXIDLE_EXPIRABLE:
               return new EmbeddedMaxIdleExpirableMetadata(
                     records, input.readLong(), TimeUnit.MILLISECONDS,
                     (EntryVersion) input.readObject());
            default:
               throw new IllegalStateException("Unknown metadata type " + number);
         }
      }

   }

}
