package org.infinispan.client.hotrod.impl.protocol;

import java.lang.annotation.Annotation;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.counter.impl.HotRodCounterEvent;
import org.infinispan.client.hotrod.event.impl.AbstractClientEvent;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.operations.PingResponse;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IntSet;

import io.netty.buffer.ByteBuf;

/**
 * A Hot Rod protocol encoder/decoder.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public interface Codec {

   int estimateHeaderSize(HeaderParams headerParams);

   /**
    * Writes a request header with the given parameters to the transport and
    * returns an updated header parameters.
    */
   HeaderParams writeHeader(ByteBuf buf, HeaderParams params);

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

   short readOpCode(ByteBuf buf);

   /**
    * Reads a response header from the transport and returns the status
    * of the response.
    */
   short readHeader(ByteBuf buf, double receivedOpCode, HeaderParams params, ChannelFactory channelFactory, SocketAddress serverAddress);

   AbstractClientEvent readCacheEvent(ByteBuf buf, Function<byte[], DataFormat> listenerDataFormat, short eventTypeId, ClassAllowList allowList, SocketAddress serverAddress);

   Object returnPossiblePrevValue(ByteBuf buf, short status, DataFormat dataFormat, int flags, ClassAllowList allowList, Marshaller marshaller);

   void writeClientListenerInterests(ByteBuf buf, Set<Class<? extends Annotation>> classes);

   /**
    * Reads a {@link HotRodCounterEvent} with the {@code listener-id}.
    */
   HotRodCounterEvent readCounterEvent(ByteBuf buf);

   /**
    * @return True if we can send operations after registering a listener on given channel
    */
   default boolean allowOperationsAndEvents() {
      return false;
   }

   /**
    * Iteration read for projection size
    * @param buf
    * @return
    */
   default int readProjectionSize(ByteBuf buf) {
      return 0;
   }

   /**
    * Iteration read to tell if metadata is present for entry
    * @param buf
    * @return
    */
   default short readMeta(ByteBuf buf) {
      return 0;
   }

   default void writeIteratorStartOperation(ByteBuf buf, IntSet segments, String filterConverterFactory, int batchSize,
                                            boolean metadata, byte[][] filterParameters) {
      throw new UnsupportedOperationException("This version doesn't support iterating upon entries!");
   }

   /**
    * Creates a key iterator with the given batch size if applicable. This iterator does not support removal.
    * @param remoteCache
    * @param operationsFactory
    * @param segments
    * @param batchSize
    * @param <K>
    * @return
    */
   default <K> CloseableIterator<K> keyIterator(RemoteCache<K, ?> remoteCache, OperationsFactory operationsFactory,
         IntSet segments, int batchSize) {
      throw new UnsupportedOperationException("This version doesn't support iterating upon keys!");
   }

   /**
    * Creates an entry iterator with the given batch size if applicable. This iterator does not support removal.
    * @param remoteCache
    * @param segments
    * @param batchSize
    * @param <K>
    * @param <V>
    * @return
    */
   default <K, V> CloseableIterator<Map.Entry<K, V>> entryIterator(RemoteCache<K, V> remoteCache,
         IntSet segments, int batchSize) {
      throw new UnsupportedOperationException("This version doesn't support iterating upon entries!");
   }

   /**
    * Reads the {@link MediaType} of the key during initial ping of the cache.
    */
   default MediaType readKeyType(ByteBuf buf) {
      return MediaType.APPLICATION_UNKNOWN;
   }

   /**
    * Reads the {@link MediaType} of the key during initial ping of the cache.
    */
   default MediaType readValueType(ByteBuf buf) {
      return MediaType.APPLICATION_UNKNOWN;
   }

   /**
    * Read the response code for hints of object storage in the server.
    */
   boolean isObjectStorageHinted(PingResponse pingResponse);
}
