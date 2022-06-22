package org.infinispan.hotrod.impl.operations;

import static org.infinispan.hotrod.impl.protocol.HotRodConstants.DEFAULT_CACHE_NAME_BYTES;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.hotrod.configuration.HotRodConfiguration;
import org.infinispan.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.hotrod.impl.ClientTopology;
import org.infinispan.hotrod.impl.HotRodTransport;
import org.infinispan.hotrod.impl.cache.ClientStatistics;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.hotrod.telemetry.impl.TelemetryService;

/**
 * @since 14.0
 **/
public class OperationContext {
   private final ChannelFactory channelFactory;
   private final AtomicReference<ClientTopology> clientTopology;
   private final ClientListenerNotifier listenerNotifier;
   private final HotRodConfiguration configuration;
   private final ClientStatistics clientStatistics;
   private final TelemetryService telemetryService;
   private final byte[] cacheNameBytes;
   private final String cacheName;
   private Codec codec;

   public OperationContext(ChannelFactory channelFactory, Codec codec, ClientListenerNotifier listenerNotifier,
                           HotRodConfiguration configuration, ClientStatistics clientStatistics,
                           TelemetryService telemetryService, String cacheName) {
      this.channelFactory = channelFactory;
      this.codec = codec;
      this.listenerNotifier = listenerNotifier;
      this.configuration = configuration;
      this.clientStatistics = clientStatistics;
      this.telemetryService = telemetryService;
      this.cacheName = cacheName;
      this.cacheNameBytes = cacheName == null ? DEFAULT_CACHE_NAME_BYTES : HotRodTransport.cacheNameBytes(cacheName);
      this.clientTopology = channelFactory != null ?
            channelFactory.createTopologyId(cacheNameBytes) :
            new AtomicReference<>(new ClientTopology(-1, configuration.clientIntelligence()));
   }

   public ChannelFactory getChannelFactory() {
      return channelFactory;
   }

   public AtomicReference<ClientTopology> getClientTopology() {
      return clientTopology;
   }

   public ClientListenerNotifier getListenerNotifier() {
      return listenerNotifier;
   }

   public HotRodConfiguration getConfiguration() {
      return configuration;
   }

   public ClientStatistics getClientStatistics() {
      return clientStatistics;
   }

   public TelemetryService getTelemetryService() {
      return telemetryService;
   }

   public byte[] getCacheNameBytes() {
      return cacheNameBytes;
   }

   public String getCacheName() {
      return cacheName;
   }

   public Codec getCodec() {
      return codec;
   }

   public void setCodec(Codec codec) {
      this.codec = codec;
   }
}
