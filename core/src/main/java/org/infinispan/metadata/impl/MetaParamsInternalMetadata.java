package org.infinispan.metadata.impl;

import org.infinispan.commons.api.functional.MetaParam;
import org.infinispan.commons.api.functional.MetaParam.Lifespan;
import org.infinispan.commons.api.functional.MetaParam.MaxIdle;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.functional.impl.MetaParams;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

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
      return 0;  // TODO: Customise this generated block
   }

   @Override
   public long lastUsed() {
      return 0;  // TODO: Customise this generated block
   }

   @Override
   public boolean isExpired(long now) {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public long expiryTime() {
      return 0;  // TODO: Customise this generated block
   }

   @Override
   public long lifespan() {
      return params.find(Lifespan.ID)
            .orElse(Lifespan.defaultValue()).get();
   }

   @Override
   public long maxIdle() {
      return params.find(MaxIdle.ID)
            .orElse(MaxIdle.defaultValue()).get();
   }

   @Override
   public EntryVersion version() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public Builder builder() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public <T> T getMetaParam(MetaParam.Id<T> id) throws NoSuchElementException {
      return params.get(id);
   }

   @Override
   public <T> Optional<T> findMetaParam(MetaParam.Id<T> id) {
      return params.find(id);
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
