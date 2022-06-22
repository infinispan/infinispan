package org.infinispan.hotrod.impl.operations;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.Set;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheEntryVersion;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.cache.CacheEntryImpl;
import org.infinispan.hotrod.impl.cache.CacheEntryMetadataImpl;
import org.infinispan.hotrod.impl.cache.CacheEntryVersionImpl;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Corresponds to getWithMetadata operation as described by
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @since 14.0
 */
public class GetWithMetadataOperation<K, V> extends AbstractKeyOperation<K, CacheEntry<K, V>> implements RetryAwareCompletionStage<CacheEntry<K, V>> {
   private static final Log log = LogFactory.getLog(GetWithMetadataOperation.class);

   private final SocketAddress preferredServer;
   private volatile boolean retried;

   public GetWithMetadataOperation(OperationContext operationContext, K key, byte[] keyBytes,
                                   CacheOptions options,
                                   DataFormat dataFormat,
                                   SocketAddress preferredServer) {
      super(operationContext, GET_WITH_METADATA, GET_WITH_METADATA_RESPONSE, key, keyBytes, options, dataFormat);
      this.preferredServer = preferredServer;
   }

   public RetryAwareCompletionStage<CacheEntry<K, V>> internalExecute() {
      // The super.execute returns this, so the cast is safe
      //noinspection unchecked
      return (RetryAwareCompletionStage<CacheEntry<K, V>>) super.execute();
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendArrayOperation(channel, keyBytes);
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      if (retryCount == 0 && preferredServer != null) {
         operationContext.getChannelFactory().fetchChannelAndInvoke(preferredServer, this);
      } else {
         retried = retryCount != 0;
         super.fetchChannelAndInvoke(retryCount, failedServers);
      }
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isNotExist(status) || !HotRodConstants.isSuccess(status)) {
         statsDataRead(false);
         complete(null);
         return;
      }
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
      V value = dataFormat().valueToObj(ByteBufUtil.readArray(buf), operationContext.getConfiguration().getClassAllowList());
      statsDataRead(true);
      complete(new CacheEntryImpl<>((K) key, value, new CacheEntryMetadataImpl(creation, lastUsed, expiration, version)));
   }

   @Override
   public Boolean wasRetried() {
      return isDone() ? retried : null;
   }
}
