package org.infinispan.client.hotrod.impl.operations;

import java.util.Objects;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;

public abstract class AbstractCacheOperation<V> extends AbstractHotRodOperation<V> {
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

   @Override
   public long timeout() {
      return internalRemoteCache.getTimeout();
   }

   /**
    * The timeout to apply to operations which are not bound to a single entry and therefore may take considerably
    * longer than {@link Configuration#socketTimeout()}. An explicit override set through
    * {@link org.infinispan.client.hotrod.RemoteCache#withTimeout(long, java.util.concurrent.TimeUnit)} always wins.
    *
    * @return the timeout in milliseconds, always greater than zero
    */
   protected final long longRunningTimeout() {
      long timeout = internalRemoteCache.getTimeout();
      return timeout > 0 ? timeout : getConfiguration().longRunningOperationTimeout();
   }

   public Configuration getConfiguration() {
      return internalRemoteCache.getRemoteCacheContainer().getConfiguration();
   }
}
