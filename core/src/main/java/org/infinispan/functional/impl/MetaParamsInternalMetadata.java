package org.infinispan.functional.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.functional.MetaParam;
import org.infinispan.functional.MetaParam.MetaCreated;
import org.infinispan.functional.MetaParam.MetaEntryVersion;
import org.infinispan.functional.MetaParam.MetaLastUsed;
import org.infinispan.functional.MetaParam.MetaLifespan;
import org.infinispan.functional.MetaParam.MetaMaxIdle;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Experimental;
import org.infinispan.commons.util.Util;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;

/**
 * Metadata parameters backed internal metadata representation.
 *
 * @since 8.0
 */
@Experimental
public final class MetaParamsInternalMetadata implements InternalMetadata, MetaParam.Lookup {

   final MetaParams params;

   public static Metadata from(MetaParams params) {
      return new MetaParamsInternalMetadata(params);
   }

   private MetaParamsInternalMetadata(MetaParams params) {
      this.params = params;
   }

   @Override
   public long created() {
      return params.find(MetaCreated.class).map(mc -> mc.get()).orElse(0L);
   }

   @Override
   public long lastUsed() {
      return params.find(MetaLastUsed.class).map(ml -> ml.get()).orElse(0L);
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

   @Override
   public long lifespan() {
      return params.find(MetaLifespan.class)
            .orElse(MetaLifespan.defaultValue()).get();
   }

   @Override
   public long maxIdle() {
      return params.find(MetaMaxIdle.class)
            .orElse(MetaMaxIdle.defaultValue()).get();
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

   @Override
   public String toString() {
      return "MetaParamsInternalMetadata{" +
         "params=" + params +
         '}';
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
      public MetaParamsInternalMetadata build() {
         return new MetaParamsInternalMetadata(params);
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
         }
         return this;
      }
   }

   public static final class Externalizer extends AbstractExternalizer<MetaParamsInternalMetadata> {
      @Override
      public void writeObject(UserObjectOutput oo, MetaParamsInternalMetadata o) throws IOException {
         MetaParams.writeTo(oo, o.params);
      }

      @Override
      public MetaParamsInternalMetadata readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         MetaParams params = MetaParams.readFrom(input);
         return new MetaParamsInternalMetadata(params);
      }

      @Override
      public Set<Class<? extends MetaParamsInternalMetadata>> getTypeClasses() {
         return Util.<Class<? extends MetaParamsInternalMetadata>>asSet(MetaParamsInternalMetadata.class);
      }

      @Override
      public Integer getId() {
         return Ids.META_PARAMS_INTERNAL_METADATA;
      }
   }

}
