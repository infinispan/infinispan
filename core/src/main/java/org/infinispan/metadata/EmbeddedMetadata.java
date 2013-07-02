package org.infinispan.metadata;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.jboss.marshalling.util.IdentityIntMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Metadata class for embedded caches.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class EmbeddedMetadata implements Metadata {

   final EntryVersion version;

   private EmbeddedMetadata(EntryVersion version) {
      this.version = version;
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
   public Metadata.Builder builder() {
      return new Builder().version(version);
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
            '}';
   }

   public static class Builder implements Metadata.Builder {

      protected long lifespan = -1;
      protected TimeUnit lifespanUnit = TimeUnit.MILLISECONDS;
      protected long maxIdle = -1;
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
         if (lifespan < 0 && maxIdle < 0)
            return new EmbeddedMetadata(version);
         else
            return new EmbeddedExpirableMetadata(
               lifespan, lifespanUnit, maxIdle, maxIdleUnit, version);
      }

   }

   private static class EmbeddedExpirableMetadata extends EmbeddedMetadata {

      private final long lifespan;
      private final long maxIdle;

      private EmbeddedExpirableMetadata(
            long lifespan, TimeUnit lifespanUnit,
            long maxIdle, TimeUnit maxIdleUnit, EntryVersion version) {
         super(version);
         this.lifespan = lifespanUnit.toMillis(lifespan);
         this.maxIdle = maxIdleUnit.toMillis(maxIdle);
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
         return new EmbeddedMetadata.Builder()
               .lifespan(lifespan).maxIdle(lifespan).version(version);
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
               '}';
      }
   }

   public static class Externalizer extends AbstractExternalizer<EmbeddedMetadata> {

      private static final int IMMORTAL = 0;
      private static final int EXPIRABLE = 1;
      private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<Class<?>>(2);

      public Externalizer() {
         numbers.put(EmbeddedMetadata.class, IMMORTAL);
         numbers.put(EmbeddedExpirableMetadata.class, EXPIRABLE);
      }

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends EmbeddedMetadata>> getTypeClasses() {
         return Util.asSet(EmbeddedMetadata.class, EmbeddedExpirableMetadata.class);
      }

      @Override
      public Integer getId() {
         return Ids.EMBEDDED_METADATA;
      }

      @Override
      public void writeObject(ObjectOutput output, EmbeddedMetadata object) throws IOException {
         int number = numbers.get(object.getClass(), -1);
         output.write(number);
         switch (number) {
            case EXPIRABLE:
               output.writeLong(object.lifespan());
               output.writeLong(object.maxIdle());
               break;
         }

         output.writeObject(object.version());
      }

      @Override
      public EmbeddedMetadata readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int number = input.readUnsignedByte();
         switch (number) {
            case IMMORTAL:
               return new EmbeddedMetadata(
                     (EntryVersion) input.readObject());
            case EXPIRABLE:
               long lifespan = input.readLong();
               long maxIdle = input.readLong();
               EntryVersion version = (EntryVersion) input.readObject();
               return new EmbeddedExpirableMetadata(
                     lifespan, TimeUnit.MILLISECONDS,
                     maxIdle, TimeUnit.MILLISECONDS, version);
            default:
               throw new IllegalStateException("Unknown metadata type " + number);
         }
      }

   }

}
