package org.infinispan.client.hotrod.impl.multimap.operations;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHeaderParams;
import org.infinispan.client.hotrod.impl.operations.AbstractKeyOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * Base class for multimap operations that manipulate keys.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public abstract class AbstractMultimapKeyOperation<V> extends AbstractKeyOperation<V> {

   protected AbstractMultimapKeyOperation(Codec codec, TransportFactory transportFactory, Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, cfg);
   }

   @Override
   protected HeaderParams createHeader() {
      return new MultimapHeaderParams();
   }
}
