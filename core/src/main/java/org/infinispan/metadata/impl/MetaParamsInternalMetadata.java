package org.infinispan.metadata.impl;

import org.infinispan.commons.api.functional.MetaParam;
import org.infinispan.commons.api.functional.MetaParam.Lifespan;
import org.infinispan.commons.api.functional.MetaParam.MaxIdle;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.functional.impl.MetaParams;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;

import java.util.NoSuchElementException;
import java.util.Optional;

public class MetaParamsInternalMetadata implements InternalMetadata, MetaParam.Lookup {

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

}
