package org.infinispan.client.hotrod.impl.operations;

import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import net.jcip.annotations.Immutable;

/**
 * Streaming put operation
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
@Immutable
public class PutStreamOperation extends AbstractKeyOperation<OutputStream> {
   static final long VERSION_PUT = 0;
   static final long VERSION_PUT_IF_ABSENT = -1;
   private final long version;
   private final long lifespan;
   private final long maxIdle;
   private final TimeUnit lifespanTimeUnit;
   private final TimeUnit maxIdleTimeUnit;
   private boolean retryable;

   public PutStreamOperation(Codec codec, TransportFactory transportFactory,
                             Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId,
                             int flags, Configuration cfg, long version,
                             long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId,
         flags, cfg);
      this.version = version;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.lifespanTimeUnit = lifespanTimeUnit;
      this.maxIdleTimeUnit = maxIdleTimeUnit;
      retryable = true;
   }

   @Override
   public OutputStream executeOperation(Transport transport) {
      HeaderParams params = writeHeader(transport, PUT_STREAM_REQUEST);
      transport.writeArray(keyBytes);
      codec.writeExpirationParams(transport, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      transport.writeLong(version);
      retryable = false;
      transport.setBusy(true);
      return codec.writeAsStream(transport, () -> {
         try {
            short status = readHeaderAndValidate(transport, params);
            if (!HotRodConstants.isSuccess(status)) {
               if (HotRodConstants.isNotExecuted(status) && (version != VERSION_PUT))
                  return;
               throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
            }
         } finally {
            transport.setBusy(false);
            transport.getTransportFactory().releaseTransport(transport);
         }
      });
   }

   @Override
   protected boolean shouldRetry(int retryCount) {
      return retryable && super.shouldRetry(retryCount);
   }
}
