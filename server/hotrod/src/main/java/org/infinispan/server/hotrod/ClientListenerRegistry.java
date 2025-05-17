package org.infinispan.server.hotrod;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.TranscoderMarshallerAdapter;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.BloomFilter;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.notifications.cachelistener.filter.KeyValueFilterConverterAsCacheEventFilterConverter;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.util.KeyValuePair;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;

/**
 * @author Galder Zamarreño
 */
class ClientListenerRegistry {
   private final EncoderRegistry encoderRegistry;
   private final Executor nonBlockingExecutor;

   ClientListenerRegistry(EncoderRegistry encoderRegistry, Executor nonBlockingExecutor) {
      this.encoderRegistry = encoderRegistry;
      this.nonBlockingExecutor = nonBlockingExecutor;
   }

   private static final Log log = LogFactory.getLog(ClientListenerRegistry.class, Log.class);

   private final ConcurrentMap<WrappedByteArray, Object> eventSenders = new ConcurrentHashMap<>();
   private final ConcurrentMap<String, CacheEventFilterFactory> cacheEventFilterFactories = new ConcurrentHashMap<>(4, 0.9f, 16);
   private final ConcurrentMap<String, CacheEventConverterFactory> cacheEventConverterFactories = new ConcurrentHashMap<>(4, 0.9f, 16);
   private final ConcurrentMap<String, CacheEventFilterConverterFactory> cacheEventFilterConverterFactories = new ConcurrentHashMap<>(4, 0.9f, 16);

   void setEventMarshaller(Optional<Marshaller> eventMarshaller) {
      eventMarshaller.ifPresent(m -> {
         TranscoderMarshallerAdapter adapter = new TranscoderMarshallerAdapter(m);
         if (encoderRegistry.isConversionSupported(MediaType.APPLICATION_OBJECT, m.mediaType())) {
            log.skippingMarshallerWrapping(m.mediaType().toString());
         } else {
            encoderRegistry.registerTranscoder(adapter);
         }
      });
   }

   void addCacheEventFilterFactory(String name, CacheEventFilterFactory factory) {
      if (factory instanceof CacheEventConverterFactory) {
         throw log.illegalFilterConverterEventFactory(name);
      }
      cacheEventFilterFactories.put(name, factory);
   }

   void removeCacheEventFilterFactory(String name) {
      cacheEventFilterFactories.remove(name);
   }

   void addCacheEventConverterFactory(String name, CacheEventConverterFactory factory) {
      if (factory instanceof CacheEventFilterFactory) {
         throw log.illegalFilterConverterEventFactory(name);
      }
      cacheEventConverterFactories.put(name, factory);
   }

   void removeCacheEventConverterFactory(String name) {
      cacheEventConverterFactories.remove(name);
   }

   void addCacheEventFilterConverterFactory(String name, CacheEventFilterConverterFactory factory) {
      cacheEventFilterConverterFactories.put(name, factory);
   }

   void removeCacheEventFilterConverterFactory(String name) {
      cacheEventFilterConverterFactories.remove(name);
   }

   CompletionStage<Void> addClientListener(Channel ch, HotRodHeader h, byte[] listenerId,
                          AdvancedCache<byte[], byte[]> cache, boolean includeState,
                          String filterFactory, List<byte[]> binaryFilterParams,
                          String converterFactory, List<byte[]> binaryConverterParams,
                          boolean useRawData, int listenerInterests, BloomFilter<byte[]> bloomFilter) {

      CacheEventFilter<byte[], byte[]> filter;
      CacheEventConverter<byte[], byte[], byte[]> converter;
      ClientEventType eventType;

      if (bloomFilter != null) {
         assert filterFactory == null || filterFactory.isEmpty();
         assert converterFactory == null || converterFactory.isEmpty();
         assert !includeState;
         eventType = ClientEventType.createType(false, useRawData, h.version);
         filter = null;
         converter = new KeyValueFilterConverterAsCacheEventFilterConverter<>(HotRodServer.ToEmptyBytesKeyValueFilterConverter.INSTANCE, cache.getKeyDataConversion().getRequestMediaType());
      } else {
         boolean hasFilter = filterFactory != null && !filterFactory.isEmpty();
         boolean hasConverter = converterFactory != null && !converterFactory.isEmpty();
         eventType = ClientEventType.createType(hasConverter, useRawData, h.version);


         if (hasFilter) {
            if (hasConverter) {
               if (filterFactory.equals(converterFactory)) {
                  List<byte[]> binaryParams = binaryFilterParams.isEmpty() ? binaryConverterParams : binaryFilterParams;
                  CacheEventFilterConverter<byte[], byte[], byte[]> filterConverter = getFilterConverter(h.getValueMediaType(),
                        filterFactory, useRawData, binaryParams);
                  filter = filterConverter;
                  converter = filterConverter;
               } else {
                  filter = getFilter(h.getValueMediaType(), filterFactory, useRawData, binaryFilterParams);
                  converter = getConverter(h.getValueMediaType(), converterFactory, useRawData, binaryConverterParams);
               }
            } else {
               filter = getFilter(h.getValueMediaType(), filterFactory, useRawData, binaryFilterParams);
               converter = null;
            }
         } else if (hasConverter) {
            filter = null;
            converter = getConverter(h.getValueMediaType(), converterFactory, useRawData, binaryConverterParams);
         } else {
            filter = null;
            converter = null;
         }
      }
      BaseClientEventSender clientEventSender = getClientEventSender(includeState, ch, h.encoder(), h.version, cache,
                                                                     listenerId, eventType, h.messageId, bloomFilter);

      eventSenders.put(new WrappedByteArray(listenerId), clientEventSender);

      return addCacheListener(cache, clientEventSender, filter, converter, listenerInterests, useRawData)
            .whenComplete((__, t) -> {
               if (t != null) {
                  // Don't try to remove the listener when
                  eventSenders.remove(new WrappedByteArray(listenerId));
               }
            });
   }

   private CompletionStage<Void> addCacheListener(AdvancedCache<byte[], byte[]> cache, Object clientEventSender,
                                 CacheEventFilter<byte[], byte[]> filter, CacheEventConverter<byte[], byte[], byte[]> converter,
                                 int listenerInterests, boolean useRawData) {
      Set<Class<? extends Annotation>> filterAnnotations;
      if (listenerInterests == 0x00) {
         filterAnnotations = new HashSet<>(Arrays.asList(
               CacheEntryCreated.class, CacheEntryModified.class,
               CacheEntryRemoved.class, CacheEntryExpired.class));
      } else {
         filterAnnotations = new HashSet<>();
         if ((listenerInterests & 0x01) == 0x01)
            filterAnnotations.add(CacheEntryCreated.class);
         if ((listenerInterests & 0x02) == 0x02)
            filterAnnotations.add(CacheEntryModified.class);
         if ((listenerInterests & 0x04) == 0x04)
            filterAnnotations.add(CacheEntryRemoved.class);
         if ((listenerInterests & 0x08) == 0x08)
            filterAnnotations.add(CacheEntryExpired.class);
      }

      // If no filter or converter are supplied, we can apply a converter so we don't have to return the value - since
      // events will only use the key
      if (converter == null && filter == null) {
         converter = new KeyValueFilterConverterAsCacheEventFilterConverter<>(HotRodServer.ToEmptyBytesKeyValueFilterConverter.INSTANCE);
         // We have to use storage format - otherwise passing converer will force it to change to incorrect format
         return cache.addStorageFormatFilteredListenerAsync(clientEventSender, filter, converter, filterAnnotations);
      } else if (useRawData) {
         return cache.addStorageFormatFilteredListenerAsync(clientEventSender, filter, converter, filterAnnotations);
      } else {
         return cache.addFilteredListenerAsync(clientEventSender, filter, converter, filterAnnotations);
      }
   }

   private CacheEventFilter<byte[], byte[]> getFilter(MediaType requestMedia, String name, Boolean useRawData, List<byte[]> binaryParams) {
      CacheEventFilterFactory factory = findFactory(name, cacheEventFilterFactories, "key/value filter");
      List<?> params = unmarshallParams(requestMedia, binaryParams, useRawData);
      return factory.getFilter(params.toArray());
   }

   private CacheEventConverter<byte[], byte[], byte[]> getConverter(MediaType requestMedia, String name, Boolean useRawData, List<byte[]> binaryParams) {
      CacheEventConverterFactory factory = findFactory(name, cacheEventConverterFactories, "converter");
      List<?> params = unmarshallParams(requestMedia, binaryParams, useRawData);
      return factory.getConverter(params.toArray());
   }

   private CacheEventFilterConverter<byte[], byte[], byte[]> getFilterConverter(MediaType requestMedia, String name, boolean useRawData, List<byte[]> binaryParams) {
      CacheEventFilterConverterFactory factory = findFactory(name, cacheEventFilterConverterFactories, "converter");
      List<?> params = unmarshallParams(requestMedia, binaryParams, useRawData);
      return factory.getFilterConverter(params.toArray());
   }

   private <T> T findFactory(String name, ConcurrentMap<String, T> factories, String factoryType) {

      T factory = factories.get(name);
      if (factory == null) throw log.missingCacheEventFactory(factoryType, name);

      return factory;
   }

   private List<?> unmarshallParams(MediaType requestMedia, List<byte[]> binaryParams, boolean useRawData) {
      if (useRawData) return binaryParams;
      return binaryParams.stream().map(bp -> encoderRegistry.convert(bp, requestMedia, APPLICATION_OBJECT)).collect(Collectors.toList());
   }

   CompletionStage<Boolean> removeClientListener(byte[] listenerId, Cache cache) {
      Object sender = eventSenders.remove(new WrappedByteArray(listenerId));
      if (sender != null) {
         // No permission check needed: Either the client had the LISTEN permission to add the listener,
         // or the listener was never added and removing it is a no-op
         return SecurityActions.removeListenerAsync(cache, sender)
                               .thenCompose(ignore -> CompletableFutures.completedTrue());
      } else return CompletableFutures.completedFalse();
   }

   public void stop() {
      eventSenders.clear();
      cacheEventFilterFactories.clear();
      cacheEventConverterFactories.clear();
   }

   void findAndWriteEvents(Channel channel) {
      // Make sure we write any event in main event loop
      channel.eventLoop().execute(() -> eventSenders.values().forEach(s -> {
         if (s instanceof BaseClientEventSender) {
            BaseClientEventSender bces = (BaseClientEventSender) s;
            if (bces.hasChannel(channel)) bces.writeEventsIfPossible();
         }
      }));
   }

   // Do not make sync=false, instead move cache operation causing
   // listener calls out of the Netty event loop thread
   @Listener(clustered = true, includeCurrentState = true)
   private class StatefulClientEventSender extends BaseClientEventSender {
      private final long messageId;

      StatefulClientEventSender(Cache cache, Channel ch, VersionedEncoder encoder, byte[] listenerId, byte version, ClientEventType targetEventType, long messageId) {
         super(cache, ch, encoder, listenerId, version, targetEventType);
         this.messageId = messageId;
      }

      @Override
      protected long getEventId(CacheEntryEvent event) {
         return event.isCurrentState() ? messageId : 0;
      }
   }

   @Listener(clustered = true)
   private class StatelessClientEventSender extends BaseClientEventSender {

      StatelessClientEventSender(Cache cache, Channel ch, VersionedEncoder encoder, byte[] listenerId, byte version, ClientEventType targetEventType) {
         super(cache, ch, encoder, listenerId, version, targetEventType);
      }
   }

   @Listener(clustered = true)
   private class BloomAwareStatelessClientEventSender extends StatelessClientEventSender {
      private final BloomFilter<byte[]> bloomFilter;

      BloomAwareStatelessClientEventSender(Cache cache, Channel ch, VersionedEncoder encoder, byte[] listenerId,
                                           byte version, ClientEventType targetEventType, BloomFilter<byte[]> bloomFilter) {
         super(cache, ch, encoder, listenerId, version, targetEventType);
         this.bloomFilter = bloomFilter;
      }

      boolean isSendEvent(CacheEntryEvent<byte[], byte[]> event) {
         if (super.isSendEvent(event)) {
            if (bloomFilter.possiblyPresent(event.getKey())) {
               if (log.isTraceEnabled()) {
                  log.tracef("Event %s passed bloom filter", event);
               }
               return true;
            } else if (log.isTraceEnabled()) {
               log.tracef("Event %s didn't pass bloom filter", event);
            }
         }
         return false;
      }
   }

   private abstract class BaseClientEventSender {
      protected final Channel ch;
      protected final VersionedEncoder encoder;
      protected final byte[] listenerId;
      protected final byte version;
      protected final ClientEventType targetEventType;
      protected final Cache cache;

      final int maxQueueSize = 100;
      final AtomicInteger eventSize = new AtomicInteger();
      final Queue<Events.Event> eventQueue = new ConcurrentLinkedQueue<>();

      private final Runnable writeEventsIfPossible = this::writeEventsIfPossible;

      BaseClientEventSender(Cache cache, Channel ch, VersionedEncoder encoder, byte[] listenerId, byte version, ClientEventType targetEventType) {
         this.cache = cache;
         this.ch = ch;
         this.encoder = encoder;
         this.listenerId = listenerId;
         this.version = version;
         this.targetEventType = targetEventType;
      }

      void init() {
         ch.closeFuture().addListener(f -> {
            log.debug("Channel disconnected, removing event sender listener for id: " + Util.printArray(listenerId));
            removeClientListener(listenerId, cache)
                  .whenComplete((ignore, t) -> unblockCommands());
         });
      }

      private void unblockCommands() {
         // Have to allow all waiting listeners to proceed
         for (Events.Event event : eventQueue) {
            event.eventFuture.complete(null);
         }
      }

      boolean hasChannel(Channel channel) {
         return ch == channel;
      }

      // This method can only be invoked from the Event Loop thread!
      void writeEventsIfPossible() {
         boolean submittedUnblock = false;
         boolean written = false;
         while (!eventQueue.isEmpty() && ch.isWritable()) {
            eventSize.decrementAndGet();
            Events.Event event = eventQueue.remove();
            if (log.isTraceEnabled()) log.tracef("Write event: %s to channel %s", event, ch);
            CompletableFuture<Void> cf = event.eventFuture;
            // We can just check instance equality as this is used to symbolize the event was not blocked below
            if (cf != CompletableFutures.<Void>completedNull()) {
               nonBlockingExecutor.execute(() -> event.eventFuture.complete(null));
            }
            ByteBuf buf = ch.alloc().ioBuffer();
            encoder.writeEvent(event, buf);
            ch.write(buf);
            written = true;
         }
         if (written) {
            ch.flush();
         }
      }

      @CacheEntryCreated
      @CacheEntryModified
      @CacheEntryRemoved
      @CacheEntryExpired
      public CompletionStage<Void> onCacheEvent(CacheEntryEvent<byte[], byte[]> event) {
         if (isSendEvent(event)) {
            long version;
            Metadata metadata;
            if ((metadata = event.getMetadata()) != null && metadata.version() != null) {
               version = ((NumericVersion) metadata.version()).getVersion();
            } else {
               version = 0;
            }
            Object k = event.getKey();
            Object v = event.getValue();
            return sendEvent((byte[]) k, (byte[]) v, version, event);
         }
         return null;
      }

      boolean isSendEvent(CacheEntryEvent<byte[], byte[]> event) {
         if (isChannelDisconnected()) {
            log.debug("Channel disconnected, ignoring event");
            return false;
         } else {
            switch (event.getType()) {
               case CACHE_ENTRY_CREATED:
               case CACHE_ENTRY_MODIFIED:
               case CACHE_ENTRY_REMOVED:
                  return !event.isPre();
               case CACHE_ENTRY_EXPIRED:
                  return true;
               default:
                  throw log.unexpectedEvent(event);
            }
         }
      }

      boolean isChannelDisconnected() {
         return !ch.isOpen();
      }

      CompletionStage<Void> sendEvent(byte[] key, byte[] value, long dataVersion, CacheEntryEvent event) {
         EventLoop loop = ch.eventLoop();
         int size = eventSize.incrementAndGet();
         boolean forceWait = size >= maxQueueSize;
         final CompletableFuture<Void> cf;
         if (forceWait) {
            if (log.isTraceEnabled()) {
               log.tracef("Pending event size is %s which is forcing %s to delay operation until it is sent", size, event);
            }

            cf = new CompletableFuture<>();
         } else {
            cf = CompletableFutures.completedNull();
         }
         Events.Event remoteEvent = createRemoteEvent(key, value, dataVersion, event, cf);

         if (log.isTraceEnabled())
            log.tracef("Queue event %s, before queuing event queue size is %d", remoteEvent, size - 1);
         eventQueue.add(remoteEvent);

         if (ch.isWritable()) {
            // Make sure we write any event in main event loop
            loop.submit(writeEventsIfPossible);
         }

         return cf;
      }

      private Events.Event createRemoteEvent(byte[] key, byte[] value, long dataVersion, CacheEntryEvent event,
            CompletableFuture<Void> eventFuture) {
         // Embedded listener event implementation implements all interfaces,
         // so can't pattern match on the event instance itself. Instead, pattern
         // match on the type and the cast down to the expected event instance type
         switch (targetEventType) {
            case PLAIN:
               switch (event.getType()) {
                  case CACHE_ENTRY_CREATED:
                  case CACHE_ENTRY_MODIFIED:
                     KeyValuePair<HotRodOperation, Boolean> responseType = getEventResponseType(event);
                     return new Events.KeyWithVersionEvent(version, getEventId(event), responseType.getKey(), listenerId,
                           responseType.getValue(), key, dataVersion, eventFuture);
                  case CACHE_ENTRY_REMOVED:
                  case CACHE_ENTRY_EXPIRED:
                     responseType = getEventResponseType(event);
                     return new Events.KeyEvent(version, getEventId(event), responseType.getKey(), listenerId,
                           responseType.getValue(), key, eventFuture);
                  default:
                     throw log.unexpectedEvent(event);
               }
            case CUSTOM_PLAIN:
               KeyValuePair<HotRodOperation, Boolean> responseType = getEventResponseType(event);
               return new Events.CustomEvent(version, getEventId(event), responseType.getKey(), listenerId,
                     responseType.getValue(), value, eventFuture);
            case CUSTOM_RAW:
               responseType = getEventResponseType(event);
               return new Events.CustomRawEvent(version, getEventId(event), responseType.getKey(), listenerId,
                     responseType.getValue(), value, eventFuture);
            default:
               throw new IllegalArgumentException("Event type not supported: " + targetEventType);
         }
      }

      protected long getEventId(CacheEntryEvent event) {
         return 0;
      }

      private KeyValuePair<HotRodOperation, Boolean> getEventResponseType(CacheEntryEvent event) {
         switch (event.getType()) {
            case CACHE_ENTRY_CREATED:
               return new KeyValuePair<>(HotRodOperation.CACHE_ENTRY_CREATED_EVENT,
                     ((CacheEntryCreatedEvent) event).isCommandRetried());
            case CACHE_ENTRY_MODIFIED:
               return new KeyValuePair<>(HotRodOperation.CACHE_ENTRY_MODIFIED_EVENT,
                     ((CacheEntryModifiedEvent) event).isCommandRetried());
            case CACHE_ENTRY_REMOVED:
               return new KeyValuePair<>(HotRodOperation.CACHE_ENTRY_REMOVED_EVENT,
                     ((CacheEntryRemovedEvent) event).isCommandRetried());
            case CACHE_ENTRY_EXPIRED:
               return new KeyValuePair<>(HotRodOperation.CACHE_ENTRY_EXPIRED_EVENT, false);
            default:
               throw log.unexpectedEvent(event);
         }
      }
   }

   private BaseClientEventSender getClientEventSender(boolean includeState, Channel ch, VersionedEncoder encoder,
                                                      byte version, Cache cache, byte[] listenerId,
                                                      ClientEventType eventType, long messageId,
                                                      BloomFilter<byte[]> bloomFilter) {
      BaseClientEventSender bces;
      if (includeState) {
         bces = new StatefulClientEventSender(cache, ch, encoder, listenerId, version, eventType, messageId);
      } else {
         if (bloomFilter != null) {
            bces = new BloomAwareStatelessClientEventSender(cache, ch, encoder, listenerId, version, eventType,
                                                            bloomFilter);
         } else {
            bces = new StatelessClientEventSender(cache, ch, encoder, listenerId, version, eventType);
         }
      }

      bces.init();

      return bces;
   }

}

enum ClientEventType {
   PLAIN,
   CUSTOM_PLAIN,
   CUSTOM_RAW;

   static ClientEventType createType(boolean isCustom, boolean useRawData, byte version) {
      if (isCustom) {
         if (useRawData && HotRodVersion.HOTROD_21.isAtLeast(version)) {
            return CUSTOM_RAW;
         }
         return CUSTOM_PLAIN;
      }
      return PLAIN;
   }
}
