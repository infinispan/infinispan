package org.infinispan.client.hotrod.impl.operations;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.VersionedMetadata;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.impl.VersionedMetadataImpl;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import net.jcip.annotations.Immutable;

/**
 * Streaming Get operation
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
@Immutable
public class GetStreamOperation<T extends InputStream & VersionedMetadata> extends AbstractKeyOperation<T> {
   private final int offset;
   private boolean retryable;

   public GetStreamOperation(Codec codec, TransportFactory transportFactory,
                             Object key, byte[] keyBytes, int offset, byte[] cacheName, AtomicInteger topologyId, int flags, ClientIntelligence clientIntelligence) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, clientIntelligence);
      this.offset = offset;
      retryable = true;
   }

   @Override
   public T executeOperation(Transport transport) {
      HeaderParams params = writeHeader(transport, GET_STREAM_REQUEST);
      transport.writeArray(keyBytes);
      transport.writeVInt(offset);
      transport.flush();
      short status = readHeaderAndValidate(transport, params);
      T result = null;
      if (HotRodConstants.isNotExist(status)) {
         result = null;
      } else {
         if (HotRodConstants.isSuccess(status)) {
            retryable = false;
            short flags = transport.readByte();
            long creation = -1;
            int lifespan = -1;
            long lastUsed = -1;
            int maxIdle = -1;
            if ((flags & INFINITE_LIFESPAN) != INFINITE_LIFESPAN) {
               creation = transport.readLong();
               lifespan = transport.readVInt();
            }
            if ((flags & INFINITE_MAXIDLE) != INFINITE_MAXIDLE) {
               lastUsed = transport.readLong();
               maxIdle = transport.readVInt();
            }
            long version = transport.readLong();
            transport.setBusy(true);
            result = codec.readAsStream(transport,
                  new VersionedMetadataImpl(creation, lifespan, lastUsed, maxIdle, version),
                  () -> {
                     transport.setBusy(false);
                     transport.getTransportFactory().releaseTransport(transport);
                  }
            );
         }
      }
      return result;
   }

   @Override
   protected boolean shouldRetry(int retryCount) {
      return retryable && super.shouldRetry(retryCount);
   }
}
