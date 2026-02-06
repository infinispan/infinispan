package org.infinispan.client.hotrod.impl.protocol;

import java.lang.annotation.Annotation;
import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.counter.impl.HotRodCounterEvent;
import org.infinispan.client.hotrod.event.impl.AbstractClientEvent;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.util.IntSet;

import io.netty.buffer.ByteBuf;

/**
 * A Hot Rod protocol encoder/decoder.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public interface Codec {

   /**
    * Writes a request header with the given parameters to the transport and
    * returns an updated header parameters.
    */
   void writeHeader(ByteBuf buf, long messageId, ClientTopology clientTopology, HotRodOperation<?> operation);

   /**
    * Writes client listener parameters
    */
   void writeClientListenerParams(ByteBuf buf, ClientListener clientListener,
                                  byte[][] filterFactoryParams, byte[][] converterFactoryParams);

   /**
    * Write lifespan/maxidle parameters.
    */
   void writeExpirationParams(ByteBuf buf, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit);

   void writeBloomFilter(ByteBuf buf, int bloomFilterBits);

   int estimateExpirationSize(long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit);

   long readMessageId(ByteBuf buf);

   void checkForErrorsInResponseStatus(ByteBuf buf, String cacheName, long messageId, short status, SocketAddress serverAddress);

   AbstractClientEvent readCacheEvent(ByteBuf buf, long messageId, Function<byte[], DataFormat> listenerDataFormat, short eventTypeId, ClassAllowList allowList, SocketAddress serverAddress);

   Object returnPossiblePrevValue(ByteBuf buf, short status, CacheUnmarshaller unmarshaller);

   <V> MetadataValue<V> returnMetadataValue(ByteBuf buf, short status, CacheUnmarshaller unmarshaller);

   void writeClientListenerInterests(ByteBuf buf, Set<Class<? extends Annotation>> classes);

   /**
    * Reads a {@link HotRodCounterEvent} with the {@code listener-id}.
    */
   HotRodCounterEvent readCounterEvent(ByteBuf buf);

   default void writeIteratorStartOperation(ByteBuf buf, IntSet segments, String filterConverterFactory, int batchSize,
                                            boolean metadata, byte[][] filterParameters) {
      throw new UnsupportedOperationException("This version doesn't support iterating upon entries!");
   }

   /**
    *
    * @param buf buffer which supportsDuplicates info will be written to.
    * @param supportsDuplicates to see whether multimap cache supports duplicates or not.
    */
   void writeMultimapSupportDuplicates(ByteBuf buf, boolean supportsDuplicates);

   /**
    * Returns true if the current codec uses a latest codec version, that could be unsafe for the initial handshake.
    * This is necessary to check interoperability between versions during the protocol negotiation.
    */
   default boolean isUnsafeForTheHandshake() {
      return false;
   }
}
