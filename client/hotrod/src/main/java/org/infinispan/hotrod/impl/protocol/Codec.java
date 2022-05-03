package org.infinispan.hotrod.impl.protocol;

import java.net.SocketAddress;
import java.util.EnumSet;
import java.util.function.Function;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.events.cache.CacheEntryEventType;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.hotrod.configuration.ProtocolVersion;
import org.infinispan.hotrod.event.ClientListener;
import org.infinispan.hotrod.event.impl.AbstractClientEvent;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.cache.RemoteCache;
import org.infinispan.hotrod.impl.counter.HotRodCounterEvent;
import org.infinispan.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.hotrod.impl.operations.PingResponse;
import org.infinispan.hotrod.impl.transport.netty.ChannelFactory;

import io.netty.buffer.ByteBuf;

/**
 * A Hot Rod protocol encoder/decoder.
 *
 * @since 14.0
 */
public interface Codec {

   static Codec forProtocol(ProtocolVersion version) {
      switch (version) {
         case PROTOCOL_VERSION_20:
            return new Codec20();
         case PROTOCOL_VERSION_21:
            return new Codec21();
         case PROTOCOL_VERSION_22:
            return new Codec22();
         case PROTOCOL_VERSION_23:
            return new Codec23();
         case PROTOCOL_VERSION_24:
            return new Codec24();
         case PROTOCOL_VERSION_25:
            return new Codec25();
         case PROTOCOL_VERSION_26:
            return new Codec26();
         case PROTOCOL_VERSION_27:
            return new Codec27();
         case PROTOCOL_VERSION_28:
            return new Codec28();
         case PROTOCOL_VERSION_29:
            return new Codec29();
         case PROTOCOL_VERSION_30:
            return new Codec30();
         case PROTOCOL_VERSION_31:
         case PROTOCOL_VERSION_AUTO:
            return new Codec31();
         default:
            throw new IllegalArgumentException(version.toString());
      }
   }

   int estimateHeaderSize(HeaderParams headerParams);

   /**
    * Writes a request header with the given parameters to the transport and returns an updated header parameters.
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
   void writeExpirationParams(ByteBuf buf, CacheEntryExpiration.Impl expiration);

   void writeBloomFilter(ByteBuf buf, int bloomFilterBits);

   int estimateExpirationSize(CacheEntryExpiration.Impl expiration);

   long readMessageId(ByteBuf buf);

   short readOpCode(ByteBuf buf);

   /**
    * Reads a response header from the transport and returns the status of the response.
    */
   short readHeader(ByteBuf buf, double receivedOpCode, HeaderParams params, ChannelFactory channelFactory, SocketAddress serverAddress);

   AbstractClientEvent readCacheEvent(ByteBuf buf, Function<byte[], DataFormat> listenerDataFormat, short eventTypeId, ClassAllowList allowList, SocketAddress serverAddress);

   Object returnPossiblePrevValue(ByteBuf buf, short status, DataFormat dataFormat, int flags, ClassAllowList allowList, Marshaller marshaller);

   void writeClientListenerInterests(ByteBuf buf, EnumSet<CacheEntryEventType> types);

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
    *
    * @param buf
    * @return
    */
   default int readProjectionSize(ByteBuf buf) {
      return 0;
   }

   /**
    * Iteration read to tell if metadata is present for entry
    *
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
    *
    * @param remoteCache
    * @param cacheOperationsFactory
    * @param segments
    * @param batchSize
    * @param <K>
    * @return
    */
   default <K> CloseableIterator<K> keyIterator(RemoteCache<K, ?> remoteCache, CacheOperationsFactory cacheOperationsFactory,
                                                CacheOptions options, IntSet segments, int batchSize) {
      throw new UnsupportedOperationException("This version doesn't support iterating upon keys!");
   }

   /**
    * Creates an entry iterator with the given batch size if applicable. This iterator does not support removal.
    *
    * @param remoteCache
    * @param segments
    * @param batchSize
    * @param <K>
    * @param <V>
    * @return
    */
   default <K, V> CloseableIterator<CacheEntry<K, V>> entryIterator(RemoteCache<K, V> remoteCache,
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
