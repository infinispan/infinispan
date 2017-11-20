package org.infinispan.client.hotrod.counter.operation;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.logging.Log;
import org.infinispan.counter.exception.CounterException;

/**
 * A base operation class for the counter's operation.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
abstract class BaseCounterOperation<T> extends RetryOnFailureOperation<T> {

   private static final Log commonsLog = LogFactory.getLog(BaseCounterOperation.class, Log.class);
   private static final byte[] EMPTY_CACHE_NAME = new byte[0];
   private static final byte[] COUNTER_CACHE_NAME = RemoteCacheManager.cacheNameBytes("org.infinispan.counter");
   private final String counterName;

   BaseCounterOperation(Codec codec, TransportFactory transportFactory, AtomicInteger topologyId, Configuration cfg,
         String counterName) {
      super(codec, transportFactory, EMPTY_CACHE_NAME, topologyId, 0, cfg);
      this.counterName = counterName;
   }

   /**
    * Writes the operation header followed by the counter's name.
    *
    * @return the {@link HeaderParams}.
    */
   HeaderParams writeHeaderAndCounterName(Transport transport, short opCode) {
      HeaderParams params = writeHeader(transport, opCode);
      transport.writeString(counterName);
      return params;
   }

   /**
    * Reads the reply header and return the operation status.
    * <p>
    * If the status is {@link #KEY_DOES_NOT_EXIST_STATUS}, the counter is undefined and a {@link CounterException} is
    * thrown.
    *
    * @return the operation's status.
    */
   short readHeaderAndValidateCounter(Transport transport, HeaderParams headerParams) {
      setCacheName(headerParams);
      short status = readHeaderAndValidate(transport, headerParams);
      if (status == KEY_DOES_NOT_EXIST_STATUS) {
         throw commonsLog.undefinedCounter(counterName);
      }
      return status;
   }

   void setCacheName(HeaderParams params) {
      params.cacheName(COUNTER_CACHE_NAME);
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(failedServers, COUNTER_CACHE_NAME);
   }
}
