package org.infinispan.client.hotrod.impl.operations;

import java.util.Objects;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;

public abstract class AbstractCacheOperation<V> extends HotRodOperation<V> {
   protected final InternalRemoteCache<?, ?> internalRemoteCache;

   protected AbstractCacheOperation(InternalRemoteCache<?, ?> internalRemoteCache) {
      this.internalRemoteCache = Objects.requireNonNull(internalRemoteCache);
   }

   @Override
   public byte[] getCacheNameBytes() {
      return internalRemoteCache.getNameBytes();
   }

   @Override
   public String getCacheName() {
      return internalRemoteCache.getName();
   }

   @Override
   public DataFormat getDataFormat() {
      return internalRemoteCache.getDataFormat();
   }

   @Override
   public int flags() {
      return internalRemoteCache.flagInt();
   }

   public Configuration getConfiguration() {
      return internalRemoteCache.getRemoteCacheContainer().getConfiguration();
   }
}
