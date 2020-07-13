package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.commons.util.Util;

import io.netty.buffer.ByteBuf;
import net.jcip.annotations.Immutable;

/**
 * Basic class for all hot rod operations that manipulate a key.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public abstract class AbstractKeyOperation<T> extends StatsAffectingRetryingOperation<T> {
   protected final Object key;
   protected final byte[] keyBytes;

   protected AbstractKeyOperation(short requestCode, short responseCode, Codec codec, ChannelFactory channelFactory,
                                  Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId, int flags,
                                  Configuration cfg, DataFormat dataFormat, ClientStatistics clientStatistics) {
      super(requestCode, responseCode, codec, channelFactory, cacheName, topologyId, flags, cfg, dataFormat, clientStatistics);
      this.key = key;
      this.keyBytes = keyBytes;
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      if (retryCount == 0) {
         channelFactory.fetchChannelAndInvoke(key == null ? keyBytes : key, failedServers, cacheName, this);
      } else {
         channelFactory.fetchChannelAndInvoke(failedServers, cacheName, this);
      }
   }

   protected T returnPossiblePrevValue(ByteBuf buf, short status) {
      return (T) codec.returnPossiblePrevValue(buf, status, dataFormat, flags, cfg.getClassAllowList(), channelFactory.getMarshaller());
   }

   protected VersionedOperationResponse returnVersionedOperationResponse(ByteBuf buf, short status) {
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
      Object prevValue = returnPossiblePrevValue(buf, status);
      return new VersionedOperationResponse(prevValue, code);
   }

   @Override
   protected void addParams(StringBuilder sb) {
      sb.append(", key=").append(key == null ? Util.printArray(keyBytes) : key);
   }
}
