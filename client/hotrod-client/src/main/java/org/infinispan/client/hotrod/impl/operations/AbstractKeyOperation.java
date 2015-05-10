package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.commons.logging.BasicLogFactory;
import org.infinispan.commons.util.Util;
import org.jboss.logging.BasicLogger;

/**
 * Basic class for all hot rod operations that manipulate a key.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public abstract class AbstractKeyOperation<T> extends RetryOnFailureOperation<T> {

   private static final BasicLogger log = BasicLogFactory.getLog(AbstractKeyOperation.class);

   protected final byte[] key;

   protected AbstractKeyOperation(Codec codec, TransportFactory transportFactory,
            byte[] key, byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(codec, transportFactory, cacheName, topologyId, flags);
      this.key = key;
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      if (retryCount == 0) {
         return transportFactory.getTransport(key, failedServers, cacheName);
      } else {
         return transportFactory.getTransport(failedServers, cacheName);
      }
   }

   protected short sendKeyOperation(byte[] key, Transport transport, byte opCode, byte opRespCode) {
      // 1) write [header][key length][key]
      HeaderParams params = writeHeader(transport, opCode);
      transport.writeArray(key);
      transport.flush();

      // 2) now read the header
      return readHeaderAndValidate(transport, params);
   }

   protected byte[] returnPossiblePrevValue(Transport transport, short status) {
      return codec.returnPossiblePrevValue(transport, status, flags);
   }

   protected VersionedOperationResponse returnVersionedOperationResponse(Transport transport, HeaderParams params) {
      //3) ...
      short respStatus = readHeaderAndValidate(transport, params);

      //4 ...
      VersionedOperationResponse.RspCode code;
      if (respStatus == NO_ERROR_STATUS || respStatus == SUCCESS_WITH_PREVIOUS) {
         code = VersionedOperationResponse.RspCode.SUCCESS;
      } else if (respStatus == NOT_PUT_REMOVED_REPLACED_STATUS || respStatus == NOT_EXECUTED_WITH_PREVIOUS) {
         code = VersionedOperationResponse.RspCode.MODIFIED_KEY;
      } else if (respStatus == KEY_DOES_NOT_EXIST_STATUS) {
         code = VersionedOperationResponse.RspCode.NO_SUCH_KEY;
      } else {
         throw new IllegalStateException("Unknown response status: " + Integer.toHexString(respStatus));
      }
      byte[] prevValue = returnPossiblePrevValue(transport, respStatus);
      return new VersionedOperationResponse(prevValue, code);
   }
}
