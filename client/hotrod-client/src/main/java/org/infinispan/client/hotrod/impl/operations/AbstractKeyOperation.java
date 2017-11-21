package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import net.jcip.annotations.Immutable;

/**
 * Basic class for all hot rod operations that manipulate a key.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public abstract class AbstractKeyOperation<T> extends RetryOnFailureOperation<T> {
   protected final Object key;
   protected final byte[] keyBytes;

   protected AbstractKeyOperation(Codec codec, TransportFactory transportFactory,
         Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg) {
      super(codec, transportFactory, cacheName, topologyId, flags, cfg);
      this.key = key;
      this.keyBytes = keyBytes;
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      if (retryCount == 0) {
         return transportFactory.getTransport(key == null ? keyBytes : key, failedServers, cacheName);
      } else {
         return transportFactory.getTransport(failedServers, cacheName);
      }
   }

   protected short sendKeyOperation(byte[] key, Transport transport, short opCode, short opRespCode) {
      // 1) write [header][key length][key]
      HeaderParams params = writeHeader(transport, opCode);
      transport.writeArray(key);
      transport.flush();

      // 2) now read the header
      return readHeaderAndValidate(transport, params);
   }

   protected T returnPossiblePrevValue(Transport transport, short status) {
      return (T) codec.returnPossiblePrevValue(transport, status, flags, cfg.serialWhitelist());
   }

   protected VersionedOperationResponse returnVersionedOperationResponse(Transport transport, HeaderParams params) {
      //3) ...
      short respStatus = readHeaderAndValidate(transport, params);

      //4 ...
      VersionedOperationResponse.RspCode code;
      if (HotRodConstants.isSuccess(respStatus)) {
         code = VersionedOperationResponse.RspCode.SUCCESS;
      } else if (HotRodConstants.isNotExecuted(respStatus)) {
         code = VersionedOperationResponse.RspCode.MODIFIED_KEY;
      } else if (HotRodConstants.isNotExist(respStatus)) {
         code = VersionedOperationResponse.RspCode.NO_SUCH_KEY;
      } else {
         throw new IllegalStateException("Unknown response status: " + Integer.toHexString(respStatus));
      }
      Object prevValue = returnPossiblePrevValue(transport, respStatus);
      return new VersionedOperationResponse(prevValue, code);
   }
}
