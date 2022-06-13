package org.infinispan.hotrod.impl.operations;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.Set;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheEntryVersion;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.util.Util;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.VersionedOperationResponse;
import org.infinispan.hotrod.impl.cache.CacheEntryImpl;
import org.infinispan.hotrod.impl.cache.CacheEntryMetadataImpl;
import org.infinispan.hotrod.impl.cache.CacheEntryVersionImpl;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;

import io.netty.buffer.ByteBuf;

/**
 * Basic class for all hot rod operations that manipulate a key.
 *
 * @since 14.0
 */
public abstract class AbstractKeyOperation<K, T> extends StatsAffectingRetryingOperation<T> {
   protected final K key;
   protected final byte[] keyBytes;

   protected AbstractKeyOperation(OperationContext operationContext, short requestCode, short responseCode,
                                  K key, byte[] keyBytes, CacheOptions options,
                                  DataFormat dataFormat) {
      super(operationContext, requestCode, responseCode, options, dataFormat);
      this.key = key;
      this.keyBytes = keyBytes;
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      if (retryCount == 0) {
         operationContext.getChannelFactory().fetchChannelAndInvoke(key == null ? keyBytes : key, failedServers, operationContext.getCacheNameBytes(), this);
      } else {
         operationContext.getChannelFactory().fetchChannelAndInvoke(failedServers, operationContext.getCacheNameBytes(), this);
      }
   }

   // This T is only ever Void or CacheEntry so the cast is safe
   protected <V> CacheEntry<K, V> returnPossiblePrevValue(ByteBuf buf, short status) {
      return operationContext.getCodec().returnPossiblePrevValue(key, buf, status, dataFormat, flags(),
            operationContext.getConfiguration().getClassAllowList(), operationContext.getChannelFactory().getMarshaller());
   }

   protected <V> VersionedOperationResponse<CacheEntry<K, V>> returnVersionedOperationResponse(ByteBuf buf, short status) {
      VersionedOperationResponse.RspCode code;
      if (HotRodConstants.isSuccess(status)) {
         code = VersionedOperationResponse.RspCode.SUCCESS;
      } else if (HotRodConstants.isNotExecuted(status)) {
         code = VersionedOperationResponse.RspCode.MODIFIED_KEY;
      } else if (HotRodConstants.isNotExist(status)) {
         code = VersionedOperationResponse.RspCode.NO_SUCH_KEY;
      } else {
         throw new IllegalStateException("Unknown response status: " + Integer.toHexString(status));
      }
      CacheEntry<K, V> prevValue = returnPossiblePrevValue(buf, status);
      return new VersionedOperationResponse<>(prevValue, code);
   }

   @Override
   protected void addParams(StringBuilder sb) {
      sb.append(", key=").append(key == null ? Util.printArray(keyBytes) : key);
   }

   public static <K, V> CacheEntry<K, V> readEntry(ByteBuf buf, K key, DataFormat dataFormat, ClassAllowList allowList) {
      short flags = buf.readUnsignedByte();
      long creation = -1;
      int lifespan = -1;
      long lastUsed = -1;
      int maxIdle = -1;
      if ((flags & INFINITE_LIFESPAN) != INFINITE_LIFESPAN) {
         creation = buf.readLong();
         lifespan = ByteBufUtil.readVInt(buf);
      }
      if ((flags & INFINITE_MAXIDLE) != INFINITE_MAXIDLE) {
         lastUsed = buf.readLong();
         maxIdle = ByteBufUtil.readVInt(buf);
      }
      CacheEntryExpiration expiration;
      if (lifespan < 0) {
         if (maxIdle < 0) {
            expiration = CacheEntryExpiration.IMMORTAL;
         } else {
            expiration = CacheEntryExpiration.withMaxIdle(Duration.ofSeconds(maxIdle));
         }
      } else {
         if (maxIdle < 0) {
            expiration = CacheEntryExpiration.withLifespan(Duration.ofSeconds(lifespan));
         } else {
            expiration = CacheEntryExpiration.withLifespanAndMaxIdle(Duration.ofSeconds(lifespan), Duration.ofSeconds(maxIdle));
         }
      }
      CacheEntryVersion version = new CacheEntryVersionImpl(buf.readLong());
      if (log.isTraceEnabled()) {
         log.tracef("Received version: %d", version);
      }
      V value = dataFormat.valueToObj(ByteBufUtil.readArray(buf), allowList);
      return new CacheEntryImpl<>(key, value, new CacheEntryMetadataImpl(creation, lastUsed, expiration, version));
   }
}
