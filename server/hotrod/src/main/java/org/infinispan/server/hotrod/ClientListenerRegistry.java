package org.infinispan.server.hotrod;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.TranscoderMarshallerAdapter;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.threads.DefaultThreadFactory;
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
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.util.KeyValuePair;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author Galder Zamarreño
 */
class ClientListenerRegistry {
   private final EncoderRegistry encoderRegistry;

   ClientListenerRegistry(EncoderRegistry encoderRegistry) {
      this.encoderRegistry = encoderRegistry;
   }

   private final static Log log = LogFactory.getLog(ClientListenerRegistry.class, Log.class);
   private final static boolean isTrace = log.isTraceEnabled();

   private final ConcurrentMap<WrappedByteArray, Object> eventSenders = new ConcurrentHashMap<>();
   private final ConcurrentMap<String, CacheEventFilterFactory> cacheEventFilterFactories = CollectionFactory.makeConcurrentMap(4, 0.9f, 16);
   private final ConcurrentMap<String, CacheEventConverterFactory> cacheEventConverterFactories = CollectionFactory.makeConcurrentMap(4, 0.9f, 16);
   private final ConcurrentMap<String, CacheEventFilterConverterFactory> cacheEventFilterConverterFactories = CollectionFactory.makeConcurrentMap(4, 0.9f, 16);

   private final ExecutorService addListenerExecutor = new ThreadPoolExecutor(
         0, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
         new DefaultThreadFactory(null, 1, "add-listener-thread-%t", null, null));

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

   void addClientListener(CacheRequestProcessor cacheProcessor, Channel ch, HotRodHeader h, byte[] listenerId,
                          AdvancedCache<byte[], byte[]> cache, boolean includeState,
                          String filterFactory, List<byte[]> binaryFilterParams,
                          String converterFactory, List<byte[]> binaryConverterParams,
                          boolean useRawData, int listenerInterests) {
      boolean hasFilter = filterFactory != null && !filterFactory.isEmpty();
      boolean hasConverter = converterFactory != null && !converterFactory.isEmpty();
      ClientEventType eventType = ClientEventType.createType(hasConverter, useRawData, h.version);

      CacheEventFilter<byte[], byte[]> filter;
      CacheEventConverter<byte[], byte[], byte[]> converter;
      if (hasFilter) {
         if (hasConverter) {
            if (filterFactory.equals(converterFactory)) {
               List<byte[]> binaryParams = binaryFilterParams.isEmpty() ? binaryConverterParams : binaryFilterParams;
               CacheEventFilterConverter<byte[], byte[], byte[]> filterConverter = getFilterConverter(cache.getValueDataConversion(), h.getValueMediaType(),
                     filterFactory, useRawData, binaryParams);
               filter = filterConverter;
               converter = filterConverter;
            } else {
               filter = getFilter(cache.getValueDataConversion(), h.getValueMediaType(), filterFactory, useRawData, binaryFilterParams);
               converter = getConverter(cache.getValueDataConversion(), h.getValueMediaType(), converterFactory, useRawData, binaryConverterParams);
            }
         } else {
            filter = getFilter(cache.getValueDataConversion(), h.getValueMediaType(), filterFactory, useRawData, binaryFilterParams);
            converter = null;
         }
      } else if (hasConverter) {
         filter = null;
         converter = getConverter(cache.getValueDataConversion(), h.getValueMediaType(), converterFactory, useRawData, binaryConverterParams);
      } else {
         filter = null;
         converter = null;
      }
      Object clientEventSender = getClientEventSender(includeState, ch, h.encoder(), h.version, cache, listenerId, eventType, h.messageId);

      eventSenders.put(new WrappedByteArray(listenerId), clientEventSender);

      if (includeState) {
         // If state included, do it async
         CompletableFuture<Void> cf = CompletableFuture.runAsync(() ->
               addCacheListener(cache, clientEventSender, filter, converter, listenerInterests, useRawData), addListenerExecutor);

         cf.whenComplete((t, cause) -> {
            if (cause != null) {
               if (cause instanceof CompletionException) {
                  cacheProcessor.writeException(h, cause.getCause());
               } else {
                  cacheProcessor.writeException(h, cause);
               }
            } else {
               cacheProcessor.writeSuccess(h);
            }
         });
      } else {
         addCacheListener(cache, clientEventSender, filter, converter, listenerInterests, useRawData);
         cacheProcessor.writeSuccess(h);
      }
   }

   private void addCacheListener(AdvancedCache<byte[], byte[]> cache, Object clientEventSender,
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
         cache.addStorageFormatFilteredListener(clientEventSender, filter, converter, filterAnnotations);
      } else if (useRawData) {
         cache.addStorageFormatFilteredListener(clientEventSender, filter, converter, filterAnnotations);
      } else {
         cache.addFilteredListener(clientEventSender, filter, converter, filterAnnotations);
      }
   }

   private CacheEventFilter<byte[], byte[]> getFilter(DataConversion valueDataConversion, MediaType requestMedia, String name, Boolean useRawData, List<byte[]> binaryParams) {
      CacheEventFilterFactory factory = findFactory(name, cacheEventFilterFactories, "key/value filter");
      List<?> params = unmarshallParams(valueDataConversion, requestMedia, binaryParams, useRawData);
      return factory.getFilter(params.toArray());
   }

   private CacheEventConverter<byte[], byte[], byte[]> getConverter(DataConversion valueDataConversion, MediaType requestMedia, String name, Boolean useRawData, List<byte[]> binaryParams) {
      CacheEventConverterFactory factory = findConverterFactory(name, cacheEventConverterFactories);
      List<?> params = unmarshallParams(valueDataConversion, requestMedia, binaryParams, useRawData);
      return factory.getConverter(params.toArray());
   }

   private CacheEventFilterConverter<byte[], byte[], byte[]> getFilterConverter(DataConversion valueDataConversion, MediaType requestMedia, String name, boolean useRawData, List<byte[]> binaryParams) {
      CacheEventFilterConverterFactory factory = findFactory(name, cacheEventFilterConverterFactories, "converter");
      List<?> params = unmarshallParams(valueDataConversion, requestMedia, binaryParams, useRawData);
      return factory.getFilterConverter(params.toArray());
   }

   private CacheEventConverterFactory findConverterFactory(String name, ConcurrentMap<String, CacheEventConverterFactory> factories) {
      if (name.equals("___eager-key-value-version-converter"))
         return KeyValueVersionConverterFactory.SINGLETON;
      else
         return findFactory(name, factories, "converter");
   }

   private <T> T findFactory(String name, ConcurrentMap<String, T> factories, String factoryType) {

      T factory = factories.get(name);
      if (factory == null) throw log.missingCacheEventFactory(factoryType, name);

      return factory;
   }

   private List<?> unmarshallParams(DataConversion valueDataConversion, MediaType requestMedia, List<byte[]> binaryParams, boolean useRawData) {
      if (useRawData) return binaryParams;
      return binaryParams.stream().map(bp -> valueDataConversion.convert(bp, requestMedia, APPLICATION_OBJECT)).collect(Collectors.toList());
   }

   boolean removeClientListener(byte[] listenerId, Cache cache) {
      Object sender = eventSenders.get(new WrappedByteArray(listenerId));
      if (sender != null) {
         cache.removeListener(sender);
         return true;
      } else return false;
   }

   public void stop() {
      eventSenders.clear();
      cacheEventFilterFactories.clear();
      cacheEventConverterFactories.clear();
      addListenerExecutor.shutdown();
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

   private abstract class BaseClientEventSender {
      protected final Channel ch;
      protected final VersionedEncoder encoder;
      protected final byte[] listenerId;
      protected final byte version;
      protected final ClientEventType targetEventType;
      protected final Cache cache;

      BlockingQueue<Events.Event> eventQueue = new LinkedBlockingQueue<>(100);

      private final Runnable writeEventsIfPossible = this::writeEventsIfPossible;

      BaseClientEventSender(Cache cache, Channel ch, VersionedEncoder encoder, byte[] listenerId, byte version, ClientEventType targetEventType) {
         this.cache = cache;
         this.ch = ch;
         this.encoder = encoder;
         this.listenerId = listenerId;
         this.version = version;
         this.targetEventType = targetEventType;
      }

      boolean hasChannel(Channel channel) {
         return ch == channel;
      }

      void writeEventsIfPossible() {
         boolean written = false;
         while (!eventQueue.isEmpty() && ch.isWritable()) {
            Events.Event event = eventQueue.poll();
            if (isTrace) log.tracef("Write event: %s to channel %s", event, ch);
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
      public void onCacheEvent(CacheEntryEvent<byte[], byte[]> event) {
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
            sendEvent((byte[]) k, (byte[]) v, version, event);
         }
      }

      boolean isSendEvent(CacheEntryEvent<?, ?> event) {
         if (isChannelDisconnected()) {
            log.debug("Channel disconnected, remove event sender listener");
            event.getCache().removeListener(this);
            return false;
         } else {
            switch (event.getType()) {
               case CACHE_ENTRY_CREATED:
               case CACHE_ENTRY_MODIFIED:
                  return !event.isPre();
               case CACHE_ENTRY_REMOVED:
                  CacheEntryRemovedEvent removedEvent = (CacheEntryRemovedEvent) event;
                  return !event.isPre() && removedEvent.getOldValue() != null;
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

      void sendEvent(byte[] key, byte[] value, long dataVersion, CacheEntryEvent event) {
         Events.Event remoteEvent = createRemoteEvent(key, value, dataVersion, event);
         if (isTrace)
            log.tracef("Queue event %s, before queuing event queue size is %d", remoteEvent, eventQueue.size());

         boolean waitingForFlush = !ch.isWritable();
         try {
            eventQueue.put(remoteEvent);
         } catch (InterruptedException e) {
            throw new CacheException(e);
         }

         if (!waitingForFlush) {
            // Make sure we write any event in main event loop
            ch.eventLoop().submit(writeEventsIfPossible);
         }
      }

      private Events.Event createRemoteEvent(byte[] key, byte[] value, long dataVersion, CacheEntryEvent event) {
         // Embedded listener event implementation implements all interfaces,
         // so can't pattern match on the event instance itself. Instead, pattern
         // match on the type and the cast down to the expected event instance type
         switch (targetEventType) {
            case PLAIN:
               switch (event.getType()) {
                  case CACHE_ENTRY_CREATED:
                  case CACHE_ENTRY_MODIFIED:
                     KeyValuePair<HotRodOperation, Boolean> responseType = getEventResponseType(event);
                     return new Events.KeyWithVersionEvent(version, getEventId(event), responseType.getKey(), listenerId, responseType.getValue(), key, dataVersion);
                  case CACHE_ENTRY_REMOVED:
                  case CACHE_ENTRY_EXPIRED:
                     responseType = getEventResponseType(event);
                     return new Events.KeyEvent(version, getEventId(event), responseType.getKey(), listenerId, responseType.getValue(), key);
                  default:
                     throw log.unexpectedEvent(event);
               }
            case CUSTOM_PLAIN:
               KeyValuePair<HotRodOperation, Boolean> responseType = getEventResponseType(event);
               return new Events.CustomEvent(version, getEventId(event), responseType.getKey(), listenerId, responseType.getValue(), value);
            case CUSTOM_RAW:
               responseType = getEventResponseType(event);
               return new Events.CustomRawEvent(version, getEventId(event), responseType.getKey(), listenerId, responseType.getValue(), value);
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

   private Object getClientEventSender(boolean includeState, Channel ch, VersionedEncoder encoder, byte version,
                                       Cache cache, byte[] listenerId, ClientEventType eventType, long messageId) {
      if (includeState) {
         return new StatefulClientEventSender(cache, ch, encoder, listenerId, version, eventType, messageId);
      } else {
         return new StatelessClientEventSender(cache, ch, encoder, listenerId, version, eventType);
      }
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
