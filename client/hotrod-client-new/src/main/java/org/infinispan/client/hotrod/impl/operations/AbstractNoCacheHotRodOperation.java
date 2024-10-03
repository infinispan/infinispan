package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;

public abstract class AbstractNoCacheHotRodOperation<E> extends AbstractHotRodOperation<E> {
   @Override
   public String getCacheName() {
      return HotRodConstants.DEFAULT_CACHE_NAME;
   }

   @Override
   public byte[] getCacheNameBytes() {
      return HotRodConstants.DEFAULT_CACHE_NAME_BYTES;
   }

   @Override
   public int flags() {
      return 0;
   }

   @Override
   public Object getRoutingObject() {
      return null;
   }

   @Override
   public boolean supportRetry() {
      // Operations not tied to a cache shouldn't be retried normally
      return false;
   }

   @Override
   public DataFormat getDataFormat() {
      return null;
   }
}
