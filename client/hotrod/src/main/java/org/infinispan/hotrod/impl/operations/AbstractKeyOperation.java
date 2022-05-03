package org.infinispan.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Set;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.commons.util.Util;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.VersionedOperationResponse;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;

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

   protected T returnPossiblePrevValue(ByteBuf buf, short status) {
      return (T) operationContext.getCodec().returnPossiblePrevValue(buf, status, dataFormat, flags(), operationContext.getConfiguration().getClassAllowList(), operationContext.getChannelFactory().getMarshaller());
   }

   protected <V> VersionedOperationResponse<V> returnVersionedOperationResponse(ByteBuf buf, short status) {
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
