package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.telemetry.impl.TelemetryService;

import io.netty.channel.Channel;

/**
 * @since 9.4
 * @author Tristan Tarrant
 */
public abstract class StatsAffectingRetryingOperation<T> extends RetryOnFailureOperation<T> {
   protected ClientStatistics clientStatistics;
   private long startTime;

   protected StatsAffectingRetryingOperation(short requestCode, short responseCode, Codec codec, ChannelFactory channelFactory,
                                             byte[] cacheName, AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg,
                                             DataFormat dataFormat, ClientStatistics clientStatistics, TelemetryService telemetryService) {
      super(requestCode, responseCode, codec, channelFactory, cacheName, clientTopology, flags, cfg, dataFormat, telemetryService);
      this.clientStatistics = clientStatistics;
   }

   @Override
   protected void scheduleRead(Channel channel) {
      if (clientStatistics.isEnabled()) {
         startTime = clientStatistics.time();
      }
      super.scheduleRead(channel);
   }

   protected void statsDataRead(boolean success) {
      if (clientStatistics.isEnabled()) {
         clientStatistics.dataRead(success, startTime, 1);
      }
   }

   protected void statsDataRead(boolean success, int count) {
      if (clientStatistics.isEnabled() && count > 0) {
         clientStatistics.dataRead(success, startTime, count);
      }
   }

   protected void statsDataStore() {
      if (clientStatistics.isEnabled()) {
         clientStatistics.dataStore(startTime, 1);
      }
   }

   protected void statsDataStore(int count) {
      if (clientStatistics.isEnabled() && count > 0) {
         clientStatistics.dataStore(startTime, count);
      }
   }

   protected void statsDataRemove() {
      if (clientStatistics.isEnabled()) {
         clientStatistics.dataRemove(startTime, 1);
      }
   }
}
