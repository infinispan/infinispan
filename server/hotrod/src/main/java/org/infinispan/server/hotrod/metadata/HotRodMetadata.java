package org.infinispan.server.hotrod.metadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.metadata.Metadata;
import org.infinispan.server.core.ExternalizerIds;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class HotRodMetadata implements Metadata {

   public static final AdvancedExternalizer<HotRodMetadata> EXTERNALIZER = new Externalzier();
   private static final long NOT_SET = -2;
   private static final long DISABLE = -1;
   private final long lifespan;
   private final long maxIdle;
   private final EntryVersion version;
   //TODO [pruivo] can a long replace the NumericVersion?
   private final NumericVersion hotRodVersion;

   private HotRodMetadata(long lifespan, long maxIdle, EntryVersion version, NumericVersion hotRodVersion) {
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.version = version;
      this.hotRodVersion = hotRodVersion;
   }

   @Override
   public long lifespan() {
      return lifespan == NOT_SET ? DISABLE : lifespan;
   }

   @Override
   public long maxIdle() {
      return maxIdle == NOT_SET ? DISABLE : maxIdle;
   }

   @Override
   public EntryVersion version() {
      return version;
   }

   public NumericVersion hotRodVersion() {
      return hotRodVersion;
   }

   @Override
   public HotRodMetadataBuilder builder() {
      HotRodMetadataBuilder builder = new HotRodMetadataBuilder();
      if (lifespan != NOT_SET) {
         builder.lifespan(lifespan);
      }
      if (maxIdle != NOT_SET) {
         builder.maxIdle(maxIdle);
      }
      builder.version(version);
      builder.hotRodVersion(hotRodVersion);
      return builder;
   }

   @Override
   public String toString() {
      return "HotRodMetadata{" +
            "lifespan=" + lifespan +
            ", maxIdle=" + maxIdle +
            ", version=" + version +
            ", hotRodVersion=" + hotRodVersion +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      HotRodMetadata metadata = (HotRodMetadata) o;

      return lifespan == metadata.lifespan &&
            maxIdle == metadata.maxIdle &&
            (version != null ? version.equals(metadata.version) : metadata.version == null) &&
            (hotRodVersion != null ? hotRodVersion.equals(metadata.hotRodVersion) : metadata.hotRodVersion == null);
   }

   @Override
   public int hashCode() {
      int result = (int) (lifespan ^ (lifespan >>> 32));
      result = 31 * result + (int) (maxIdle ^ (maxIdle >>> 32));
      result = 31 * result + (version != null ? version.hashCode() : 0);
      result = 31 * result + (hotRodVersion != null ? hotRodVersion.hashCode() : 0);
      return result;
   }

   public static class HotRodMetadataBuilder implements Builder {

      private long lifespan = NOT_SET;
      private long maxIdle = NOT_SET;
      private EntryVersion version;
      private NumericVersion hotRodVersion;

      public HotRodMetadataBuilder() {
      }

      @Override
      public HotRodMetadataBuilder lifespan(long time, TimeUnit unit) {
         return lifespan(unit.toMillis(time));
      }

      @Override
      public HotRodMetadataBuilder lifespan(long time) {
         lifespan = time < 0 ? DISABLE : time;
         return this;
      }

      @Override
      public HotRodMetadataBuilder maxIdle(long time, TimeUnit unit) {
         return maxIdle(unit.toMillis(time));
      }

      @Override
      public HotRodMetadataBuilder maxIdle(long time) {
         maxIdle = time < 0 ? DISABLE : time;
         return this;
      }

      @Override
      public HotRodMetadataBuilder version(EntryVersion version) {
         this.version = version;
         return this;
      }

      @Override
      public HotRodMetadata build() {
         return new HotRodMetadata(lifespan, maxIdle, version, hotRodVersion);
      }

      @Override
      public HotRodMetadataBuilder merge(Metadata metadata) {
         if (lifespan == NOT_SET) {
            lifespan(metadata.lifespan());
         }
         if (maxIdle == NOT_SET) {
            maxIdle(metadata.maxIdle());
         }
         if (version == null) {
            version = metadata.version();
         }
         if (metadata instanceof HotRodMetadata && hotRodVersion == null) {
            this.hotRodVersion = ((HotRodMetadata) metadata).hotRodVersion();
         }
         return this;
      }

      public HotRodMetadataBuilder hotRodVersion(NumericVersion version) {
         hotRodVersion = version;
         return this;
      }

      @Override
      public String toString() {
         return "HotRodMetadataBuilder{" +
               "lifespan=" + lifespan +
               ", maxIdle=" + maxIdle +
               ", version=" + version +
               ", hotRodVersion=" + hotRodVersion +
               '}';
      }
   }

   private static class Externalzier implements AdvancedExternalizer<HotRodMetadata> {

      @Override
      public Set<Class<? extends HotRodMetadata>> getTypeClasses() {
         return Collections.singleton(HotRodMetadata.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.HOT_ROD_METADATA;
      }

      @Override
      public void writeObject(ObjectOutput output, HotRodMetadata object) throws IOException {
         output.writeLong(object.lifespan);
         output.writeLong(object.maxIdle);
         output.writeObject(object.version);
         output.writeObject(object.hotRodVersion);
      }

      @Override
      public HotRodMetadata readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new HotRodMetadata(input.readLong(), input.readLong(), (EntryVersion) input.readObject(), (NumericVersion) input.readObject());
      }
   }
}
