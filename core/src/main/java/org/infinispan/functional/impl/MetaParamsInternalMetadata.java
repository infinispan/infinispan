package org.infinispan.functional.impl;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Experimental;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.functional.MetaParam;
import org.infinispan.functional.MetaParam.MetaCreated;
import org.infinispan.functional.MetaParam.MetaEntryVersion;
import org.infinispan.functional.MetaParam.MetaLastUsed;
import org.infinispan.functional.MetaParam.MetaLifespan;
import org.infinispan.functional.MetaParam.MetaMaxIdle;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Metadata parameters backed internal metadata representation.
 *
 * @since 8.0
 */
@Experimental
@ProtoTypeId(ProtoStreamTypeIds.META_PARAMS_INTERNAL_METADATA)
public final class MetaParamsInternalMetadata implements InternalMetadata, MetaParam.Lookup {

   private static final MetaParamsInternalMetadata EMPTY = new MetaParamsInternalMetadata(MetaParams.empty());

   final MetaParams params;

   public static Metadata from(MetaParams params) {
      return new MetaParamsInternalMetadata(params);
   }

   @ProtoFactory
   MetaParamsInternalMetadata(NumericVersion numericVersion, SimpleClusteredVersion clusteredVersion,
                              long created, long lastUsed, long lifespan, long maxIdle, CounterConfiguration counterConfiguration,
                              boolean updateCreationTimestamp) {
      this.params = new MetaParams(MetaParams.EMPTY_ARRAY, 0);
      if (numericVersion != null || clusteredVersion != null) {
         this.params.add(new MetaEntryVersion(numericVersion == null ? clusteredVersion : numericVersion));
      }
      if (created > -1) params.add(new MetaCreated(created));
      if (lastUsed > -1) params.add(new MetaLastUsed(lastUsed));
      if (lifespan > -1) params.add(new MetaLifespan(lifespan));
      if (maxIdle > -1) params.add(new MetaMaxIdle(maxIdle));
      if (counterConfiguration != null) {
         params.add(new CounterConfigurationMetaParam(counterConfiguration));
      }
      // Default is always true, so no need to set the param unless it is false
      if (!updateCreationTimestamp) {
         params.add(MetaParam.MetaUpdateCreationTime.of(false));
      }
   }

   private MetaParamsInternalMetadata(MetaParams params) {
      this.params = params;
   }

   @ProtoField(1)
   NumericVersion getNumericVersion() {
      EntryVersion version = version();
      return version instanceof NumericVersion ? (NumericVersion) version : null;
   }

   @ProtoField(2)
   SimpleClusteredVersion getClusteredVersion() {
      EntryVersion version = version();
      return version instanceof SimpleClusteredVersion ? (SimpleClusteredVersion) version : null;
   }

   @ProtoField(number = 3, defaultValue = "-1")
   @Override
   public long created() {
      return params.find(MetaCreated.class).map(MetaParam.MetaLong::get).orElse(0L);
   }

   @ProtoField(number = 4, defaultValue = "-1")
   @Override
   public long lastUsed() {
      return params.find(MetaLastUsed.class).map(MetaParam.MetaLong::get).orElse(0L);
   }

   @Override
   public boolean isExpired(long now) {
      long expiryTime = expiryTime();
      return expiryTime >= 0 && expiryTime <= now;
   }

   @Override
   public long expiryTime() {
      long deadline = -1;
      long lifespan = lifespan();
      if (lifespan >= 0) {
         deadline = created() + lifespan;
      }
      long maxIdle = maxIdle();
      if (maxIdle >= 0) {
         if (deadline < 0) {
            deadline = lastUsed() + maxIdle;
         } else {
            deadline = Math.min(deadline, lastUsed() + maxIdle);
         }
      }
      return deadline;
   }

   @ProtoField(number = 5, defaultValue = "-1")
   @Override
   public long lifespan() {
      return params.find(MetaLifespan.class)
            .orElse(MetaLifespan.defaultValue()).get();
   }

   @ProtoField(number = 6, defaultValue = "-1")
   @Override
   public long maxIdle() {
      return params.find(MetaMaxIdle.class)
            .orElse(MetaMaxIdle.defaultValue()).get();
   }

   @ProtoField(7)
   public CounterConfiguration counterConfiguration() {
      return params.find(CounterConfigurationMetaParam.class).map(CounterConfigurationMetaParam::get).orElse(null);
   }

   @ProtoField(value = 8, defaultValue = "true")
   @Override
   public boolean updateCreationTimestamp() {
      return params.find(MetaParam.MetaUpdateCreationTime.class).map(MetaParam.MetaBoolean::get).orElse(true);
   }

   @Override
   public EntryVersion version() {
      return params.find(MetaEntryVersion.class).map(MetaEntryVersion::get).orElse(null);
   }

   @Override
   public Builder builder() {
      return new Builder(params.copy());
   }

   @Override
   public <T extends MetaParam> Optional<T> findMetaParam(Class<T> type) {
      return params.find(type);
   }

   public boolean isEmpty() {
      return params.isEmpty();
   }

   @Override
   public String toString() {
      return "MetaParamsInternalMetadata{params=" + params + '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      MetaParamsInternalMetadata that = (MetaParamsInternalMetadata) o;
      return created() == that.created() &&
            lastUsed() == that.lastUsed() &&
            lifespan() == that.lifespan() &&
            maxIdle() == that.maxIdle() &&
            Objects.equals(version(), that.version()) &&
            Objects.equals(counterConfiguration(), that.counterConfiguration());
   }

   @Override
   public int hashCode() {
      return Objects.hash(created(), lastUsed(), lastUsed(), maxIdle(), version(), counterConfiguration());
   }

   public static MetaParamsInternalMetadata.Builder getBuilder(MetaParamsInternalMetadata metadata) {
      return metadata == null ?
             new MetaParamsInternalMetadata.Builder() :
             metadata.builder();
   }

   public static MetaParamsInternalMetadata empty() {
      return EMPTY;
   }

   public static class Builder implements Metadata.Builder {
      private final MetaParams params;

      public Builder() {
         this.params = MetaParams.empty();
      }

      Builder(MetaParams params) {
         this.params = params;
      }

      @Override
      public Builder lifespan(long time, TimeUnit unit) {
         params.add(new MetaLifespan(unit.toMillis(time)));
         return this;
      }

      @Override
      public Builder lifespan(long time) {
         params.add(new MetaLifespan(time));
         return this;
      }

      @Override
      public Builder maxIdle(long time, TimeUnit unit) {
         params.add(new MetaMaxIdle(unit.toMillis(time)));
         return this;
      }

      @Override
      public Builder maxIdle(long time) {
         params.add(new MetaMaxIdle(time));
         return this;
      }

      @Override
      public Builder version(EntryVersion version) {
         params.add(new MetaEntryVersion(version));
         return this;
      }

      @Override
      public Metadata.Builder updateCreationTimestamp(boolean enabled) {
         params.add(new MetaParam.MetaUpdateCreationTime(enabled));
         return this;
      }

      @Override
      public MetaParamsInternalMetadata build() {
         return new MetaParamsInternalMetadata(params);
      }

      public Builder add(MetaParam<?> metaParam) {
         params.add(metaParam);
         return this;
      }

      @Override
      public Builder merge(Metadata metadata) {
         if (metadata instanceof MetaParamsInternalMetadata) {
            params.merge(((MetaParamsInternalMetadata) metadata).params);
         } else {
            if (!params.find(MetaLifespan.class).isPresent()) {
               lifespan(metadata.lifespan());
            }
            if (!params.find(MetaMaxIdle.class).isPresent()) {
               maxIdle(metadata.maxIdle());
            }
            if (!params.find(MetaEntryVersion.class).isPresent()) {
               version(metadata.version());
            }
            if (!params.find(MetaParam.MetaUpdateCreationTime.class).isPresent()) {
               updateCreationTimestamp(metadata.updateCreationTimestamp());
            }
         }
         return this;
      }
   }
}
