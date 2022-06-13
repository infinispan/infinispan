package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.MetadataValueImpl;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.ClassAllowList;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Corresponds to getWithMetadata operation as described by
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Immutable
public class GetWithMetadataOperation<V> extends AbstractKeyOperation<MetadataValue<V>> implements RetryAwareCompletionStage<MetadataValue<V>> {

   private static final Log log = LogFactory.getLog(GetWithMetadataOperation.class);

   private final SocketAddress preferredServer;

   private volatile boolean retried;

   public GetWithMetadataOperation(Codec codec, ChannelFactory channelFactory, Object key, byte[] keyBytes,
                                   byte[] cacheName, AtomicInteger topologyId, int flags,
                                   Configuration cfg, DataFormat dataFormat, ClientStatistics clientStatistics,
                                   SocketAddress preferredServer) {
      super(GET_WITH_METADATA, GET_WITH_METADATA_RESPONSE, codec, channelFactory, key, keyBytes, cacheName, topologyId,
            flags, cfg, dataFormat, clientStatistics);
      this.preferredServer = preferredServer;
   }

   public RetryAwareCompletionStage<MetadataValue<V>> internalExecute() {
      // The super.execute returns this, so the cast is safe
      //noinspection unchecked
      return (RetryAwareCompletionStage<MetadataValue<V>>) super.execute();
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendArrayOperation(channel, keyBytes);
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      if (retryCount == 0 && preferredServer != null) {
         channelFactory.fetchChannelAndInvoke(preferredServer, this);
      } else {
         retried = retryCount != 0;
         super.fetchChannelAndInvoke(retryCount, failedServers);
      }
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      MetadataValue<V> metadataValue = readMetadataValue(buf, status, dataFormat, cfg.getClassAllowList());
      statsDataRead(metadataValue != null);
      complete(metadataValue);
   }

   public static <V> MetadataValue<V> readMetadataValue(ByteBuf buf, short status, DataFormat dataFormat,
         ClassAllowList classAllowList) {
      if (HotRodConstants.isNotExist(status) || (!HotRodConstants.isSuccess(status) && !HotRodConstants.hasPrevious(status))) {
         return null;
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
      long version = buf.readLong();
      if (log.isTraceEnabled()) {
         log.tracef("Received version: %d", version);
      }
      V value = dataFormat.valueToObj(ByteBufUtil.readArray(buf), classAllowList);

      return new MetadataValueImpl<>(creation, lifespan, lastUsed, maxIdle, version, value);
   }

   @Override
   public Boolean wasRetried() {
      return isDone() ? retried : null;
   }
}
