package org.infinispan.server.hotrod;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.threads.DefaultThreadFactory;
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
import org.infinispan.notifications.cachelistener.filter.AbstractCacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.util.KeyValuePair;

import io.netty.channel.Channel;

/**
 * @author Galder Zamarreño
 */
class ClientListenerRegistry {
   private final HotRodServerConfiguration configuration;

   ClientListenerRegistry(HotRodServerConfiguration configuration) {
      this.configuration = configuration;
   }

   private final static Log log = LogFactory.getLog(ClientListenerRegistry.class, Log.class);
   private final static boolean isTrace = log.isTraceEnabled();

   private final AtomicLong messageId = new AtomicLong();
   private final ConcurrentMap<WrappedByteArray, Object> eventSenders = new ConcurrentHashMap<>();

   volatile private Optional<Marshaller> marshaller = Optional.empty();
   private final ConcurrentMap<String, CacheEventFilterFactory> cacheEventFilterFactories = CollectionFactory.makeConcurrentMap(4, 0.9f, 16);
   private final ConcurrentMap<String, CacheEventConverterFactory> cacheEventConverterFactories = CollectionFactory.makeConcurrentMap(4, 0.9f, 16);
   private final ConcurrentMap<String, CacheEventFilterConverterFactory> cacheEventFilterConverterFactories = CollectionFactory.makeConcurrentMap(4, 0.9f, 16);

   private final ExecutorService addListenerExecutor = new ThreadPoolExecutor(
         0, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
         new DefaultThreadFactory(null, 1, "add-listener-thread-%t", null, null));

   void setEventMarshaller(Optional<Marshaller> eventMarshaller) {
      // Set a custom marshaller or reset to default if none
      marshaller = eventMarshaller;
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

   void addClientListener(VersionedDecoder decoder, Channel ch, HotRodHeader h, byte[] listenerId,
                          AdvancedCache<byte[], byte[]> cache, boolean includeState,
                          KeyValuePair<Optional<KeyValuePair<String, List<byte[]>>>, Optional<KeyValuePair<String, List<byte[]>>>> namedFactories,
                          boolean useRawData, int listenerInterests) {
      ClientEventType eventType = ClientEventType.createType(namedFactories.getValue().isPresent(), useRawData, h.version);
      Object clientEventSender = getClientEventSender(includeState, ch, h.version, cache, listenerId, eventType);
      List<byte[]> binaryFilterParams = namedFactories.getKey().map(KeyValuePair::getValue).orElse(Collections.emptyList());
      List<byte[]> binaryConverterParams = namedFactories.getValue().map(KeyValuePair::getValue).orElse(Collections.emptyList());
      boolean compatEnabled = cache.getCacheConfiguration().compatibility().enabled();

      KeyValuePair<CacheEventFilter<byte[], byte[]>, CacheEventConverter<byte[], byte[], byte[]>> kvp;
      if (namedFactories.getKey().isPresent()) {
         KeyValuePair<String, List<byte[]>> filterFactory = namedFactories.getKey().get();
         if (namedFactories.getValue().isPresent()) {
            KeyValuePair<String, List<byte[]>> converterFactory = namedFactories.getValue().get();
            if (filterFactory.getKey().equals(converterFactory.getKey())) {
               List<byte[]> binaryParams = binaryFilterParams.isEmpty() ? binaryConverterParams : binaryFilterParams;
               CacheEventFilterConverter<byte[], byte[], byte[]> filterConverter = getFilterConverter(
                     filterFactory.getKey(), compatEnabled, useRawData, binaryParams);
               kvp = new KeyValuePair<>(filterConverter, filterConverter);
            } else {
               kvp = new KeyValuePair<>(getFilter(filterFactory.getKey(), compatEnabled, useRawData, binaryFilterParams),
                     getConverter(converterFactory.getKey(), compatEnabled, useRawData, binaryConverterParams));
            }
         } else {
            kvp = new KeyValuePair<>(getFilter(namedFactories.getKey().get().getKey(), compatEnabled, useRawData, binaryFilterParams), null);
         }
      } else {
         if (namedFactories.getValue().isPresent()) {
            kvp = new KeyValuePair<>(null, getConverter(namedFactories.getValue().get().getKey(), compatEnabled, useRawData, binaryConverterParams));
         } else {
            kvp = new KeyValuePair<>(null, null);
         }
      }

      eventSenders.put(new WrappedByteArray(listenerId), clientEventSender);

      if (includeState) {
         // If state included, do it async
         CompletableFuture<Void> cf = CompletableFuture.runAsync(() ->
               addCacheListener(cache, clientEventSender, kvp, listenerInterests), addListenerExecutor);

         cf.whenComplete((t, cause) -> {
            Response resp;
            if (cause != null) {
               if (cause instanceof CompletionException) {
                  resp = decoder.createErrorResponse(h, cause.getCause());
               } else {
                  resp = decoder.createErrorResponse(h, cause);
               }
            } else {
               resp = decoder.createSuccessResponse(h, null);
            }
            ch.writeAndFlush(resp);
         });
      } else {
         addCacheListener(cache, clientEventSender, kvp, listenerInterests);
         ch.writeAndFlush(decoder.createSuccessResponse(h, null));
      }
   }

   private void addCacheListener(AdvancedCache<byte[], byte[]> cache, Object clientEventSender,
                                 KeyValuePair<CacheEventFilter<byte[], byte[]>, CacheEventConverter<byte[], byte[], byte[]>> kvp,
                                 int listenerInterests) {
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

      cache.addFilteredListener(clientEventSender, kvp.getKey(), kvp.getValue(), filterAnnotations);
   }

   CacheEventFilter<byte[], byte[]> getFilter(String name, Boolean compatEnabled, Boolean useRawData, List<byte[]> binaryParams) {
      KeyValuePair<CacheEventFilterFactory, Marshaller> factory =
            findFactory(name, compatEnabled, cacheEventFilterFactories, "key/value filter", useRawData);
      List<? extends Object> params = unmarshallParams(binaryParams, factory.getValue(), useRawData);
      return factory.getKey().getFilter(params.toArray());
   }

   CacheEventConverter<byte[], byte[], byte[]> getConverter(String name, boolean compatEnabled, Boolean useRawData, List<byte[]> binaryParams) {
      KeyValuePair<CacheEventConverterFactory, Marshaller> factory =
            findConverterFactory(name, compatEnabled, cacheEventConverterFactories, "converter", useRawData);
      List<? extends Object> params = unmarshallParams(binaryParams, factory.getValue(), useRawData);
      return factory.getKey().getConverter(params.toArray());
   }

   CacheEventFilterConverter<byte[], byte[], byte[]> getFilterConverter(String name, boolean compatEnabled, boolean useRawData, List<byte[]> binaryParams) {
      KeyValuePair<CacheEventFilterConverterFactory, Marshaller> factory =
            findFactory(name, compatEnabled, cacheEventFilterConverterFactories, "converter", useRawData);
      List<? extends Object> params = unmarshallParams(binaryParams, factory.getValue(), useRawData);
      return factory.getKey().getFilterConverter(params.toArray());
   }

   KeyValuePair<CacheEventConverterFactory, Marshaller> findConverterFactory(String name, boolean compatEnabled,
                                                                             ConcurrentMap<String, CacheEventConverterFactory> factories, String factoryType, boolean useRawData) {
      if (name.equals("___eager-key-value-version-converter"))
         return new KeyValuePair<>(KeyValueVersionConverterFactory.SINGLETON, new GenericJBossMarshaller());
      else
         return findFactory(name, compatEnabled, factories, factoryType, useRawData);
   }

   <T> KeyValuePair<T, Marshaller> findFactory(String name, boolean compatEnabled,
                                               ConcurrentMap<String, T> factories, String factoryType, boolean useRawData) {

      T factory = factories.get(name);
      if (factory == null) throw log.missingCacheEventFactory(factoryType, name);

      Marshaller m = marshaller.orElse(new GenericJBossMarshaller(factory.getClass().getClassLoader()));
      if (useRawData || compatEnabled)
         return new KeyValuePair<>(factory, m);
      else
         return new KeyValuePair<>(createFactory(factory, m), m);
   }

   <T> T createFactory(T factory, Marshaller marshaller) {
      if (factory instanceof CacheEventConverterFactory) {
         return (T) new UnmarshallConverterFactory((CacheEventConverterFactory) factory, marshaller);
      } else if (factory instanceof CacheEventFilterFactory) {
         return (T) new UnmarshallFilterFactory((CacheEventFilterFactory) factory, marshaller);
      } else if (factory instanceof CacheEventFilterConverterFactory) {
         return (T) new UnmarshallFilterConverterFactory((CacheEventFilterConverterFactory) factory, marshaller);
      } else {
         throw new IllegalArgumentException("Unsupported factory: " + factory);
      }
   }

   private List<? extends Object> unmarshallParams(List<byte[]> binaryParams, Marshaller marshaller, boolean useRawData) {
      if (!useRawData) {
         return binaryParams.stream().map(bp -> {
            try {
               return marshaller.objectFromByteBuffer(bp);
            } catch (IOException | ClassNotFoundException e) {
               throw new CacheException(e);
            }
         }).collect(Collectors.toList());
      } else return binaryParams;
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

      protected StatefulClientEventSender(Cache cache, Channel ch, byte[] listenerId, byte version,
                                          ClientEventType targetEventType, DataConversion keyDataConversion,
                                          DataConversion valueDataConversion) {
         super(cache, ch, listenerId, version, targetEventType, keyDataConversion, valueDataConversion);
      }
   }

   @Listener(clustered = true, includeCurrentState = false)
   private class StatelessClientEventSender extends BaseClientEventSender {

      protected StatelessClientEventSender(Cache cache, Channel ch, byte[] listenerId, byte version, ClientEventType targetEventType,
                                           DataConversion keyDataConversion, DataConversion valueDataConversion) {
         super(cache, ch, listenerId, version, targetEventType, keyDataConversion, valueDataConversion);
      }
   }

   private abstract class BaseClientEventSender {
      protected final Channel ch;
      protected final byte[] listenerId;
      protected final byte version;
      protected final ClientEventType targetEventType;
      private final DataConversion keyDataConversion;
      private final DataConversion valueDataConversion;
      protected final Cache cache;

      BlockingQueue<Object> eventQueue = new LinkedBlockingQueue<>(100);

      private final Runnable writeEventsIfPossible = this::writeEventsIfPossible;

      protected BaseClientEventSender(Cache cache, Channel ch, byte[] listenerId, byte version, ClientEventType targetEventType,
                                      DataConversion keyDataConversion, DataConversion valueDataConversion) {
         this.cache = cache;
         this.ch = ch;
         this.listenerId = listenerId;
         this.version = version;
         this.targetEventType = targetEventType;
         this.keyDataConversion = keyDataConversion;
         this.valueDataConversion = valueDataConversion;
      }

      boolean hasChannel(Channel channel) {
         return ch == channel;
      }

      void writeEventsIfPossible() {
         boolean written = false;
         while (!eventQueue.isEmpty() && ch.isWritable()) {
            Object event = eventQueue.poll();
            if (isTrace) log.tracef("Write event: %s to channel %s", event, ch);
            ch.write(event);
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
            if (keyDataConversion.isStorageFormatFilterable()) {
               k = keyDataConversion.fromStorage(k);
            }
            if (valueDataConversion.isStorageFormatFilterable()) {
               v = valueDataConversion.fromStorage(v);
            }

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
         Object remoteEvent = createRemoteEvent(key, value, dataVersion, event);
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

      private Object createRemoteEvent(byte[] key, byte[] value, long dataVersion, CacheEntryEvent event) {
         long id = messageId.incrementAndGet(); // increment message id
         // Embedded listener event implementation implements all interfaces,
         // so can't pattern match on the event instance itself. Instead, pattern
         // match on the type and the cast down to the expected event instance type
         switch (targetEventType) {
            case PLAIN:
               switch (event.getType()) {
                  case CACHE_ENTRY_CREATED:
                  case CACHE_ENTRY_MODIFIED:
                     KeyValuePair<HotRodOperation, Boolean> responseType = getEventResponseType(event);
                     return keyWithVersionEvent(key, dataVersion, responseType.getKey(), responseType.getValue());
                  case CACHE_ENTRY_REMOVED:
                  case CACHE_ENTRY_EXPIRED:
                     responseType = getEventResponseType(event);
                     return new Events.KeyEvent(version, id, responseType.getKey(), listenerId, responseType.getValue(), key);
                  default:
                     throw log.unexpectedEvent(event);
               }
            case CUSTOM_PLAIN:
               KeyValuePair<HotRodOperation, Boolean> responseType = getEventResponseType(event);
               return new Events.CustomEvent(version, id, responseType.getKey(), listenerId, responseType.getValue(), value);
            case CUSTOM_RAW:
               responseType = getEventResponseType(event);
               return new Events.CustomRawEvent(version, id, responseType.getKey(), listenerId, responseType.getValue(), value);
            default:
               throw new IllegalArgumentException("Event type not supported: " + targetEventType);
         }
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

      private Events.KeyWithVersionEvent keyWithVersionEvent(byte[] key, long dataVersion, HotRodOperation op, boolean isRetried) {
         return new Events.KeyWithVersionEvent(version, messageId.get(), op, listenerId, isRetried, key, dataVersion);
      }

   }

   Object getClientEventSender(boolean includeState, Channel ch, byte version,
                               Cache cache, byte[] listenerId, ClientEventType eventType) {
      DataConversion keyDataConversion = cache.getAdvancedCache().getKeyDataConversion();
      DataConversion valueDataConversion = cache.getAdvancedCache().getValueDataConversion();
      if (includeState) {
         return new StatefulClientEventSender(cache, ch, listenerId, version, eventType, keyDataConversion, valueDataConversion);
      } else {
         return new StatelessClientEventSender(cache, ch, listenerId, version, eventType, keyDataConversion, valueDataConversion);
      }
   }

   private class UnmarshallFilterFactory implements CacheEventFilterFactory {
      private final CacheEventFilterFactory filterFactory;
      private final Marshaller marshaller;

      private UnmarshallFilterFactory(CacheEventFilterFactory filterFactory, Marshaller marshaller) {
         this.filterFactory = filterFactory;
         this.marshaller = marshaller;
      }

      @Override
      public <K, V> CacheEventFilter<K, V> getFilter(Object[] params) {
         return (CacheEventFilter<K, V>) new UnmarshallFilter(filterFactory.getFilter(params), marshaller);
      }
   }

   class UnmarshallConverterFactory implements CacheEventConverterFactory {
      private final CacheEventConverterFactory converterFactory;
      private final Marshaller marshaller;

      UnmarshallConverterFactory(CacheEventConverterFactory converterFactory, Marshaller marshaller) {
         this.converterFactory = converterFactory;
         this.marshaller = marshaller;
      }

      @Override
      public <K, V, C> CacheEventConverter<K, V, C> getConverter(Object[] params) {
         return (CacheEventConverter<K, V, C>) new UnmarshallConverter(converterFactory.getConverter(params), marshaller);
      }
   }

   class UnmarshallFilterConverterFactory implements CacheEventFilterConverterFactory {
      private final CacheEventFilterConverterFactory filterConverterFactory;
      private final Marshaller marshaller;

      UnmarshallFilterConverterFactory(CacheEventFilterConverterFactory filterConverterFactory, Marshaller marshaller) {
         this.filterConverterFactory = filterConverterFactory;
         this.marshaller = marshaller;
      }

      @Override
      public <K, V, C> CacheEventFilterConverter<K, V, C> getFilterConverter(Object[] params) {
         return (CacheEventFilterConverter<K, V, C>) new UnmarshallFilterConverter(filterConverterFactory.getFilterConverter(params), marshaller);
      }
   }

   static class UnmarshallFilter implements CacheEventFilter<byte[], byte[]> {
      private final CacheEventFilter<Object, Object> filter;
      private final Marshaller marshaller;

      UnmarshallFilter(CacheEventFilter<Object, Object> filter, Marshaller marshaller) {
         this.filter = filter;
         this.marshaller = marshaller;
      }


      @Override
      public boolean accept(byte[] key, byte[] oldValue, Metadata oldMetadata, byte[] newValue, Metadata newMetadata, EventType eventType) {
         Object unmarshalledKey;
         Object unmarshalledPrevValue;
         Object unmarshalledValue;
         try {
            unmarshalledKey = marshaller.objectFromByteBuffer(key);
            unmarshalledPrevValue = oldValue != null ? marshaller.objectFromByteBuffer(oldValue) : null;
            unmarshalledValue = newValue != null ? marshaller.objectFromByteBuffer(newValue) : null;
         } catch (IOException | ClassNotFoundException e) {
            throw new CacheException(e);
         }
         return filter.accept(unmarshalledKey, unmarshalledPrevValue, oldMetadata, unmarshalledValue, newMetadata, eventType);
      }
   }

   static class UnmarshallFilterExternalizer extends AbstractExternalizer<UnmarshallFilter> {
      @Override
      public void writeObject(ObjectOutput output, UnmarshallFilter obj) throws IOException {
         output.writeObject(obj.filter);
         output.writeObject(obj.marshaller.getClass());
      }

      @Override
      public UnmarshallFilter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         CacheEventFilter<Object, Object> filter = (CacheEventFilter<Object, Object>) input.readObject();
         Class<? extends Marshaller> marshallerClass = (Class<? extends Marshaller>) input.readObject();
         // See if the marshaller can be constructed
         Marshaller marshaller = constructMarshaller(filter, marshallerClass);
         return new UnmarshallFilter(filter, marshaller);
      }

      @Override
      public Set<Class<? extends UnmarshallFilter>> getTypeClasses() {
         return Collections.singleton(UnmarshallFilter.class);
      }
   }

   private static <T> Marshaller constructMarshaller(T t, Class<? extends Marshaller> marshallerClass) {
      Constructor<? extends Marshaller> constructor = findClassloaderConstructor(marshallerClass);
      try {
         if (constructor != null) {
            return constructor.newInstance(t.getClass().getClassLoader());
         } else {
            return marshallerClass.newInstance();
         }
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
         throw new CacheException(e);
      }
   }

   private static Constructor<? extends Marshaller> findClassloaderConstructor(Class<? extends Marshaller> clazz) {
      try {
         return clazz.getConstructor(ClassLoader.class);
      } catch (NoSuchMethodException e) {
         return null;
      }
   }

   static class UnmarshallConverter implements CacheEventConverter<byte[], byte[], byte[]> {
      private final CacheEventConverter<Object, Object, Object> converter;
      private final Marshaller marshaller;

      UnmarshallConverter(CacheEventConverter<Object, Object, Object> converter, Marshaller marshaller) {
         this.converter = converter;
         this.marshaller = marshaller;
      }

      @Override
      public byte[] convert(byte[] key, byte[] oldValue, Metadata oldMetadata, byte[] newValue, Metadata newMetadata, EventType eventType) {
         try {
            Object unmarshalledKey = marshaller.objectFromByteBuffer(key);
            Object unmarshalledPrevValue = oldValue != null ? marshaller.objectFromByteBuffer(oldValue) : null;
            Object unmarshalledValue = newValue != null ? marshaller.objectFromByteBuffer(newValue) : null;
            Object converted = converter.convert(unmarshalledKey, unmarshalledPrevValue, oldMetadata, unmarshalledValue, newMetadata, eventType);
            return marshaller.objectToByteBuffer(converted);
         } catch (IOException | ClassNotFoundException | InterruptedException e) {
            throw new CacheException(e);
         }
      }
   }

   static class UnmarshallConverterExternalizer extends AbstractExternalizer<UnmarshallConverter> {
      @Override
      public void writeObject(ObjectOutput output, UnmarshallConverter obj) throws IOException {
         output.writeObject(obj.converter);
         output.writeObject(obj.marshaller.getClass());
      }

      @Override
      public UnmarshallConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         CacheEventConverter<Object, Object, Object> converter = (CacheEventConverter<Object, Object, Object>) input.readObject();
         Class<? extends Marshaller> marshallerClass = (Class<? extends Marshaller>) input.readObject();
         Marshaller marshaller = constructMarshaller(converter, marshallerClass);
         return new UnmarshallConverter(converter, marshaller);
      }

      @Override
      public Set<Class<? extends UnmarshallConverter>> getTypeClasses() {
         return Collections.singleton(UnmarshallConverter.class);
      }
   }

   static class UnmarshallFilterConverter extends AbstractCacheEventFilterConverter<byte[], byte[], byte[]> {
      private final CacheEventFilterConverter<Object, Object, Object> filterConverter;
      private final Marshaller marshaller;

      UnmarshallFilterConverter(CacheEventFilterConverter<Object, Object, Object> filterConverter, Marshaller marshaller) {
         this.filterConverter = filterConverter;
         this.marshaller = marshaller;
      }

      @Override
      public byte[] filterAndConvert(byte[] key, byte[] oldValue, Metadata oldMetadata, byte[] newValue, Metadata newMetadata, EventType eventType) {
         try {
            Object unmarshalledKey = marshaller.objectFromByteBuffer(key);
            Object unmarshalledPrevValue = oldValue != null ? marshaller.objectFromByteBuffer(oldValue) : null;
            Object unmarshalledValue = newValue != null ? marshaller.objectFromByteBuffer(newValue) : null;
            Object converted = filterConverter.filterAndConvert(unmarshalledKey, unmarshalledPrevValue,
                  oldMetadata, unmarshalledValue, newMetadata, eventType);
            return marshaller.objectToByteBuffer(converted);
         } catch (IOException | ClassNotFoundException | InterruptedException e) {
            throw new CacheException(e);
         }
      }
   }

   static class UnmarshallFilterConverterExternalizer extends AbstractExternalizer<UnmarshallFilterConverter> {
      @Override
      public void writeObject(ObjectOutput output, UnmarshallFilterConverter obj) throws IOException {
         output.writeObject(obj.filterConverter);
         output.writeObject(obj.marshaller.getClass());
      }

      @Override
      public UnmarshallFilterConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         CacheEventFilterConverter<Object, Object, Object> filterConverter =
               (CacheEventFilterConverter<Object, Object, Object>) input.readObject();
         Class<? extends Marshaller> marshallerClass = (Class<? extends Marshaller>) input.readObject();
         Marshaller marshaller = constructMarshaller(filterConverter, marshallerClass);
         return new UnmarshallFilterConverter(filterConverter, marshaller);
      }

      @Override
      public Set<Class<? extends UnmarshallFilterConverter>> getTypeClasses() {
         return Collections.singleton(UnmarshallFilterConverter.class);
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
