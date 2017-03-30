package org.infinispan.functional.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationRecord;
import org.infinispan.commons.api.functional.MetaParam;
import org.infinispan.commons.api.functional.MetaParam.MetaCreated;
import org.infinispan.commons.api.functional.MetaParam.MetaEntryVersion;
import org.infinispan.commons.api.functional.MetaParam.MetaLastUsed;
import org.infinispan.commons.api.functional.MetaParam.MetaLifespan;
import org.infinispan.commons.api.functional.MetaParam.MetaMaxIdle;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Experimental;
import org.infinispan.commons.util.Util;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.FunctionalEntryVersionAdapter;
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
      return params.find(MetaEntryVersion.class).map(MetaParamsInternalMetadata::versionOrNull).orElse(null);
   }

   @Override
   public InvocationRecord lastInvocation() {
      return params.find(InvocationRecords.class).flatMap(InvocationRecords::lastInvocation).orElse(null);
   }

   @Override
   public InvocationRecord invocation(CommandInvocationId id) {
      return params.find(InvocationRecords.class).flatMap(ir -> ir.invocation(id)).orElse(null);
   }

   private static EntryVersion versionOrNull(MetaEntryVersion mev) {
      org.infinispan.commons.api.functional.EntryVersion entryVersion = mev.get();
      return entryVersion instanceof FunctionalEntryVersionAdapter ? ((FunctionalEntryVersionAdapter) entryVersion).get() : null;
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

      public Builder(MetaParams params) {
         this.params = params;
      }

      @Override
      public Metadata.Builder lifespan(long time, TimeUnit unit) {
         params.add(new MetaLifespan(unit.toMillis(time)));
         return this;
      }

      @Override
      public Metadata.Builder lifespan(long time) {
         params.add(new MetaLifespan(time));
         return this;
      }

      @Override
      public Metadata.Builder maxIdle(long time, TimeUnit unit) {
         params.add(new MetaMaxIdle(unit.toMillis(time)));
         return this;
      }

      @Override
      public Metadata.Builder maxIdle(long time) {
         params.add(new MetaMaxIdle(time));
         return this;
      }

      @Override
      public Metadata.Builder version(EntryVersion version) {
         params.add(new MetaEntryVersion<>(new FunctionalEntryVersionAdapter(version)));
         return this;
      }

      @Override
      public Metadata.Builder invocation(CommandInvocationId id, Object returnValue, boolean authoritative, boolean created, boolean modified, boolean removed, long timestamp) {
         params.replace(InvocationRecords.class, records -> InvocationRecords.join(id, returnValue, authoritative, created, modified, removed, timestamp, records));
         return this;
      }

      @Override
      public Metadata.Builder invocations(InvocationRecord invocations) {
         params.replace(InvocationRecords.class, ignored -> InvocationRecords.of(invocations));
         return this;
      }

      @Override
      public InvocationRecord invocations() {
         return params.find(InvocationRecords.class).flatMap(InvocationRecords::lastInvocation).orElse(null);
      }

      @Override
      public Metadata build() {
         return new MetaParamsInternalMetadata(params);
      }

      @Override
      public Metadata.Builder merge(Metadata metadata) {
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
      public void writeObject(ObjectOutput oo, MetaParamsInternalMetadata o) throws IOException {
         oo.writeObject(o.params);
      }

      @Override
      public MetaParamsInternalMetadata readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         MetaParams params = (MetaParams) input.readObject();
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
