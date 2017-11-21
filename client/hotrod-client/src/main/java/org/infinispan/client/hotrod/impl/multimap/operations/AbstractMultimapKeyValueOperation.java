package org.infinispan.client.hotrod.impl.multimap.operations;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHeaderParams;
import org.infinispan.client.hotrod.impl.operations.AbstractKeyValueOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * Base class for multimap operations that manipulate keys and values.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public abstract class AbstractMultimapKeyValueOperation<T> extends AbstractKeyValueOperation<T> {

   protected AbstractMultimapKeyValueOperation(Codec codec, TransportFactory transportFactory, Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg, byte[] value, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, cfg, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
   }

   public AbstractMultimapKeyValueOperation(Codec codec, TransportFactory transportFactory, Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg, byte[] value) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, cfg, value, -1, TimeUnit.MILLISECONDS, -1, TimeUnit.MILLISECONDS);

   }

   protected HeaderParams createHeader() {
      return new MultimapHeaderParams();
   }
}
