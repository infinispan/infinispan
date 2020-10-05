package org.infinispan.cloudevents.impl;

import static org.infinispan.cloudevents.impl.StructuredEventBuilder.isJsonPrimitive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.infinispan.cloudevents.configuration.CloudEventsGlobalConfiguration;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.encoding.impl.StorageConfigurationManager;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.LogFactory;

/**
 * Serialize cache events into cloudevents-encoded JSON.
 */
@Scope(Scopes.NAMED_CACHE)
@Listener(primaryOnly = true, observation = Listener.Observation.POST)
public class EntryEventListener {
   private static final Log log = LogFactory.getLog(EntryEventListener.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject StorageConfigurationManager storageConfigurationManager;
   @ComponentName(KnownComponentNames.PERSISTENCE_MARSHALLER)
   @Inject PersistenceMarshaller persistenceMarshaller;
   @Inject CacheNotifier<?, ?> cacheNotifier;
   @Inject Transport transport;
   @Inject KafkaEventSender kafkaEventSender;

   private Transcoder keyJsonTranscoder;
   private Transcoder valueJsonTranscoder;
   private String clusterName;
   private String cacheEntriesTopic;
   private Address localAddress;

   @Inject
   void inject(EncoderRegistry encoderRegistry, GlobalConfiguration globalConfiguration) {
      MediaType keyBytesMediaType = bytesMediaType(storageConfigurationManager.getKeyStorageMediaType(),
                                                   persistenceMarshaller.mediaType());
      if (encoderRegistry.isConversionSupported(keyBytesMediaType, MediaType.APPLICATION_JSON)) {
         keyJsonTranscoder = encoderRegistry.getTranscoder(keyBytesMediaType, MediaType.APPLICATION_JSON);
      }

      MediaType valueBytesMediaType = bytesMediaType(storageConfigurationManager.getKeyStorageMediaType(),
                                                     persistenceMarshaller.mediaType());
      if (encoderRegistry.isConversionSupported(keyBytesMediaType, MediaType.APPLICATION_JSON)) {
         valueJsonTranscoder = encoderRegistry.getTranscoder(valueBytesMediaType, MediaType.APPLICATION_JSON);
      }

      clusterName = globalConfiguration.transport().clusterName();

      CloudEventsGlobalConfiguration cloudEventsGlobalConfiguration =
            globalConfiguration.module(CloudEventsGlobalConfiguration.class);
      cacheEntriesTopic = cloudEventsGlobalConfiguration.cacheEntriesTopic();
   }

   @Start
   void start() {
      localAddress = localAddress;
      cacheNotifier.addListener(this);
   }

   @CacheEntryCreated
   @CacheEntryModified
   @CacheEntryRemoved
   @CacheEntryExpired
   public CompletionStage<Void> handleCacheEntryEvent(CacheEntryEvent<?, ?> e) {
      try {
         ProducerRecord<byte[], byte[]> record = entryEventToKafkaMessage(e);
         if (trace)
            log.tracef("Sending cloudevents message for %s %s %s event", e.getType(), e.getKey(), e.getSource());
         return kafkaEventSender.send(record);
      } catch (IOException ioException) {
         log.sendError(e.getType(), e.getKey(), e.getSource());
         return null;
      } catch (InterruptedException interruptedException) {
         if (trace) log.tracef("Cache manager is shutting down, skipping event");
         return null;
      }
   }

   private MediaType bytesMediaType(MediaType storageMediaType, MediaType persistenceMediaType) {
      return storageMediaType.match(MediaType.APPLICATION_OBJECT) ?
             persistenceMediaType : storageMediaType;
   }

   public ProducerRecord<byte[], byte[]> entryEventToKafkaMessage(CacheEntryEvent<?, ?> event)
         throws IOException, InterruptedException {
      StructuredEventBuilder writer = new StructuredEventBuilder();

      Object key = storageConfigurationManager.getKeyWrapper().unwrap(event.getKey());
      Object wrappedValue = event.getType() != Event.Type.CACHE_ENTRY_REMOVED ? event.getValue() :
                     ((CacheEntryRemovedEvent<?, ?>) event).getOldValue();
      Object value = storageConfigurationManager.getValueWrapper().unwrap(wrappedValue);

      String cacheName = event.getCache().getName();
      writer.setSource("/infinispan/" + clusterName + "/" + cacheName);
      writer.setType(translateType(event.getType()));
      writer.setTime(Instant.now().toString());

      String stringKey = writeKey(writer, key, storageConfigurationManager.getKeyStorageMediaType());
      Object source = event.getSource();
      if (source == null) {
         source = ThreadLocalRandom.current().nextLong();
      }

      // The key + the tx id (or non-tx invocation id) can uniquely identify an event
      // Consumers should ignore duplicates sent with the same id, e.g. when a command is retried
      writer.setId(stringKey + ":" + source);

      MediaType storageMediaType = storageConfigurationManager.getValueStorageMediaType();
      writeValue(writer, value, storageMediaType);
      writeVersion(event, writer);
      return writer.toKafkaRecord(cacheEntriesTopic);
   }

   private String writeKey(StructuredEventBuilder writer, Object key, MediaType storageMediaType)
         throws IOException, InterruptedException {
      String keyString = null;
      byte[] keyBytes;
      MediaType mediaType;
      boolean validUtf8;
      if (storageMediaType.match(MediaType.APPLICATION_OBJECT)) {
         if (isJsonPrimitive(key.getClass())) {
            keyString = Json.make(key).toString();
            keyBytes = keyString.getBytes(StandardCharsets.UTF_8);
            validUtf8 = true;
            mediaType = MediaType.APPLICATION_JSON;
         } else {
            byte[] storageBytes = persistenceMarshaller.getUserMarshaller().objectToByteBuffer(key);
            if (valueJsonTranscoder != null) {
               // The persistence marshaller's output can be converted to JSON
               Object jsonBytes = keyJsonTranscoder.transcode(storageBytes, storageMediaType,
                                                              MediaType.APPLICATION_JSON);
               keyBytes = (byte[]) jsonBytes;
               mediaType = MediaType.APPLICATION_JSON;
               validUtf8 = true;
            } else {
               keyBytes = storageBytes;
               mediaType = storageMediaType;
               validUtf8 = isValidUtf8(storageBytes);
            }
         }
      } else {
         keyBytes = (byte[]) key;
         mediaType = storageMediaType;
         validUtf8 = isValidUtf8(keyBytes);
      }
      if (keyString == null) {
         keyString = bytesToString(keyBytes, validUtf8);
      }
      writer.setKey(keyBytes);
      writer.setSubject(keyString, mediaType, validUtf8);
      return keyString;
   }

   private void writeValue(StructuredEventBuilder writer, Object value, MediaType storageMediaType)
         throws IOException, InterruptedException {
      if (value != null) {
         if (storageMediaType.match(MediaType.APPLICATION_OBJECT)) {
            if (isJsonPrimitive(value.getClass())) {
               writer.setPrimitiveData(value);
            } else {
               byte[] storageBytes = persistenceMarshaller.getUserMarshaller().objectToByteBuffer(value);
               if (valueJsonTranscoder != null) {
                  // The persistence marshaller's output can be converted to JSON
                  Object jsonBytes = valueJsonTranscoder.transcode(storageBytes, storageMediaType,
                                                                   MediaType.APPLICATION_JSON);
                  writer.setData((byte[]) jsonBytes, MediaType.APPLICATION_JSON,
                                 !isValidUtf8((byte[]) jsonBytes));
               } else {
                  writer.setData(storageBytes, storageMediaType, !isValidUtf8(storageBytes));
               }
            }
         } else {
            byte[] valueBytes = (byte[]) value;
            writer.setData(valueBytes, storageMediaType, !isValidUtf8(valueBytes));
         }
      } else {
         writer.setPrimitiveData(null);
      }
   }

   private String bytesToString(byte[] valueBytes, boolean validUtf8) {
      if (!validUtf8) {
         return Base64.getEncoder().encodeToString(valueBytes);
      } else {
         return new String(valueBytes, StandardCharsets.UTF_8);
      }
   }

   private void writeVersion(CacheEntryEvent<?, ?> event, StructuredEventBuilder writer)
         throws IOException, InterruptedException {
      if (event.getMetadata() != null) {
         EntryVersion version = event.getMetadata().version();
         if (version != null) {
            byte[] versionBytes = persistenceMarshaller.objectToByteBuffer(version);
            writer.setEntryVersion(versionBytes);
         }
      }
   }


   private static String translateType(Event.Type type) {
      switch (type) {
         case CACHE_ENTRY_CREATED:
            return "org.infinispan.entry.created";
         case CACHE_ENTRY_EVICTED:
            return "org.infinispan.entry.evicted";
         case CACHE_ENTRY_EXPIRED:
            return "org.infinispan.entry.expired";
         case CACHE_ENTRY_MODIFIED:
            return "org.infinispan.entry.modified";
         case CACHE_ENTRY_REMOVED:
            return "org.infinispan.entry.removed";
         default:
            throw new IllegalArgumentException("Unsupported event type: " + type);
      }
   }

   private static boolean isValidUtf8(byte[] bytes) {
      if (bytes.length == 0)
         return true;

      CharsetDecoder utf8Decoder =
            StandardCharsets.UTF_8.newDecoder()
                                  .onMalformedInput(CodingErrorAction.REPORT)
                                  .onUnmappableCharacter(CodingErrorAction.REPORT);
      ByteBuffer in = ByteBuffer.wrap(bytes);
      CharBuffer out = CharBuffer.allocate(StructuredEventBuilder.VALIDATION_BUFFER_SIZE);

      for (; ; ) {
         if (!in.hasRemaining())
            break;

         CoderResult cr = utf8Decoder.decode(in, out, true);
         if (cr.isUnderflow())
            break;

         if (cr.isOverflow()) {
            continue;
         }
         // Malformed input or unmappable character
         return false;
      }

      return true;
   }
}
