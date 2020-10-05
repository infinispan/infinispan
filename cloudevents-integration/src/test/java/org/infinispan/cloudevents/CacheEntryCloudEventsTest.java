package org.infinispan.cloudevents;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.cloudevents.impl.StructuredEventBuilder.DATA;
import static org.infinispan.cloudevents.impl.StructuredEventBuilder.DATACONTENTTYPE;
import static org.infinispan.cloudevents.impl.StructuredEventBuilder.INFINISPAN_DATA_ISBASE64;
import static org.infinispan.cloudevents.impl.StructuredEventBuilder.INFINISPAN_SUBJECT_CONTENTTYPE;
import static org.infinispan.cloudevents.impl.StructuredEventBuilder.INFINISPAN_SUBJECT_ISBASE64;
import static org.infinispan.cloudevents.impl.StructuredEventBuilder.SOURCE;
import static org.infinispan.cloudevents.impl.StructuredEventBuilder.SPECVERSION;
import static org.infinispan.cloudevents.impl.StructuredEventBuilder.SUBJECT;
import static org.infinispan.cloudevents.impl.StructuredEventBuilder.TIME;
import static org.infinispan.cloudevents.impl.StructuredEventBuilder.TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cloudevents.configuration.CloudEventsGlobalConfigurationBuilder;
import org.infinispan.cloudevents.impl.KafkaEventSender;
import org.infinispan.commands.module.TestGlobalConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.rehash.TestWriteOperation;
import org.infinispan.encoding.DataConversion;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "cloudevents.impl.CacheEntryCloudEventsTest")
public class CacheEntryCloudEventsTest extends MultipleCacheManagersTest {
   public static final String CACHE_NAME = "testCache";
   private final MockKafkaEventSender mockSender = new MockKafkaEventSender();

   private StorageType storageType;
   private boolean serverMode;

   @Override
   public Object[] factory() {
      return new Object[]{
            new CacheEntryCloudEventsTest().storageType(StorageType.OBJECT),
            new CacheEntryCloudEventsTest().storageType(StorageType.BINARY),
            new CacheEntryCloudEventsTest().storageType(StorageType.HEAP).serverMode(true),
            };
   }

   @DataProvider
   public static Object[][] operations() {
      return new Object[][]{
            {TestWriteOperation.PUT_CREATE},
            {TestWriteOperation.PUT_OVERWRITE},
            {TestWriteOperation.PUT_IF_ABSENT},
            {TestWriteOperation.REPLACE},
            {TestWriteOperation.REPLACE_EXACT},
            {TestWriteOperation.REMOVE},
            {TestWriteOperation.REMOVE_EXACT},
            {TestWriteOperation.PUT_MAP_CREATE},
            // TODO Add TestWriteOperation enum values for compute/computeIfAbsent/computeIfPresent/merge
            };
   }

   public CacheEntryCloudEventsTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   private Object serverMode(boolean serverMode) {
      this.serverMode = serverMode;
      return this;
   }

   @Override
   protected void createCacheManagers() {
      addNode();
      addNode();
      addNode();
      waitForClusterToForm();
   }

   @Override
   protected String[] parameterNames() {
      return new String[]{"storage", "server"};
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[]{storageType, serverMode ? "y" : null};
   }

   private Address addNode() {
      GlobalConfigurationBuilder managerBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      managerBuilder.defaultCacheName(CACHE_NAME).serialization().addContextInitializer(TestDataSCI.INSTANCE);
      if (serverMode) {
         managerBuilder.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      }
      CloudEventsGlobalConfigurationBuilder cloudEventsGlobalBuilder =
            managerBuilder.addModule(CloudEventsGlobalConfigurationBuilder.class);
      cloudEventsGlobalBuilder.bootstrapServers("localhost:9092");
      cloudEventsGlobalBuilder.cacheEntriesTopic("ispn");

      TestGlobalConfigurationBuilder testGlobalConfigurationBuilder =
            managerBuilder.addModule(TestGlobalConfigurationBuilder.class);
      testGlobalConfigurationBuilder.testGlobalComponent(KafkaEventSender.class.getName(), mockSender);

      ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();
      cacheBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);
      cacheBuilder.memory().storage(storageType);

      EmbeddedCacheManager manager = addClusterEnabledCacheManager(managerBuilder, cacheBuilder);
      return manager.getAddress();
   }


   @Test(dataProvider = "operations")
   public void testSingleKeyOperations(TestWriteOperation op)
         throws InterruptedException, TimeoutException, ExecutionException {
      AdvancedCache<Object, Object> originator = advancedCache(0);
      for (Cache<Object, Object> cache : caches()) {
         MagicKey key = new MagicKey(cache);
         if (op.getPreviousValue() != null) {
            mockSender.clear();
            // Skipping listener notification skips the cloudevents integration
            originator.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(key, op.getPreviousValue());
            assertTrue(mockSender.getProducer().history().isEmpty());
         }

         mockSender.clear();
         CompletionStage<?> stage = op.performAsync(originator, key);
         assertFalse(stage.toCompletableFuture().isDone());

         mockSender.completeSend();
         stage.toCompletableFuture().get(30, SECONDS);

         Object eventValue = op.getValue() != null ? op.getValue() : op.getPreviousValue();
         assertEntryEventSent(key, eventValue, op);

         Object returnValue = stage.toCompletableFuture().get(30, SECONDS);
         assertEquals(op.getReturnValue(), returnValue);
         assertEntryEventSent(key, eventValue, op);
      }
   }

   public void testMultiKeyOperations() throws InterruptedException, TimeoutException, ExecutionException {
      Map<MagicKey, Object> data = new LinkedHashMap<>();
      for (int i = 0; i < caches().size(); i++) {
         MagicKey key = new MagicKey("key-" + i, cache(i));
         String value = "value-" + i;
         data.put(key, value);
      }

      for (Cache<Object, Object> cache : caches()) {
         log.tracef("Testing on %s", address(cache));
         mockSender.clear();
         CompletableFuture<Void> putAllFuture = cache.putAllAsync(data);
         assertFalse(putAllFuture.isDone());

         mockSender.completeSend(data.size());
         ((CompletionStage<?>) putAllFuture).toCompletableFuture().get(30, SECONDS);

         data.forEach((key, value) -> {
            assertEntryEventSent(key, value, TestWriteOperation.PUT_MAP_CREATE);
         });

         mockSender.clear();
         CompletableFuture<Void> clearFuture = cache.clearAsync();
         assertFalse(clearFuture.isDone());

         mockSender.completeSend(data.size());
         ((CompletionStage<?>) clearFuture).toCompletableFuture().get(30, SECONDS);
         data.forEach((key, value) -> {
            assertEntryEventSent(key, value, TestWriteOperation.REMOVE);
         });
      }
   }

   private void assertEntryEventSent(Object key, Object value, TestWriteOperation op) {
      byte[] expectedKeyBytes = getKeyBytes(key);
      byte[] expectedValueBytes = getValueBytes(value);
      String type = translateType(op);
      Optional<ProducerRecord<byte[], byte[]>> record =
            mockSender.getProducer().history().stream()
                      .filter(r -> Arrays.equals(r.key(), expectedKeyBytes))
                      .findFirst();
      assertTrue(record.isPresent());
      byte[] eventBytes = record.get().value();
      Json json = Json.read(new String(eventBytes));
      assertEquals("1.0", json.at(SPECVERSION).asString());
      assertEquals(type, json.at(TYPE).asString());
      String source = json.at(SOURCE).asString();
      assertTrue(source.startsWith("/infinispan"));
      assertTrue(source.endsWith("/" + CACHE_NAME));
      Instant.parse(json.at(TIME).asString());

      boolean keyIsBase64 = json.at(INFINISPAN_SUBJECT_ISBASE64, false).asBoolean();
      assertEquals(expectedContentType(keyIsBase64).toString(),
                   json.at(INFINISPAN_SUBJECT_CONTENTTYPE, APPLICATION_JSON_TYPE).asString());
      String subject = json.at(SUBJECT).asString();
      byte[] keyBytes = keyIsBase64 ? Base64.getDecoder().decode(subject) :
                        subject.getBytes(StandardCharsets.UTF_8);
      assertEquals(expectedKeyBytes, keyBytes);

      String data = json.at(DATA).asString();
      boolean valueIsBase64 = json.at(INFINISPAN_DATA_ISBASE64, false).asBoolean();
      assertEquals(expectedContentType(valueIsBase64).toString(), json.at(DATACONTENTTYPE, APPLICATION_JSON_TYPE).asString());
      byte[] valueBytes = valueIsBase64 ? Base64.getDecoder().decode(data) :
                          data.getBytes(StandardCharsets.UTF_8);
      assertEquals(expectedValueBytes, valueBytes);
   }

   private MediaType expectedContentType(boolean isBase64) {
      return isBase64 ? serverMode ? APPLICATION_UNKNOWN : APPLICATION_PROTOSTREAM : APPLICATION_JSON;
   }

   private byte[] getKeyBytes(Object key) {
      DataConversion keyDataConversion = advancedCache(0).getKeyDataConversion();
      return getBytes(key, keyDataConversion);
   }

   private byte[] getValueBytes(Object value) {
      DataConversion valueDataConversion = advancedCache(0).getValueDataConversion();
      return getBytes(value, valueDataConversion);
   }

   private byte[] getBytes(Object o, DataConversion dataConversion) {
      MediaType storageMediaType = dataConversion.getStorageMediaType();
      if (storageMediaType.match(APPLICATION_OBJECT)) {
         if (o instanceof String) {
            return getBytes((String) o);
         } else {
            Object protostream = transcode(o, APPLICATION_OBJECT, APPLICATION_PROTOSTREAM);
            return (byte[]) transcode(protostream, APPLICATION_PROTOSTREAM, MediaType.APPLICATION_JSON);
         }
      } else {
         return (byte[]) dataConversion.getWrapper().unwrap(dataConversion.toStorage(o));
      }
   }

   private Object transcode(Object o, MediaType sourceMediaType, MediaType targetMediaType) {
      EncoderRegistry encoderRegistry = TestingUtil.extractGlobalComponent(manager(0), EncoderRegistry.class);
      return encoderRegistry.getTranscoder(sourceMediaType, targetMediaType)
                            .transcode(o, sourceMediaType, targetMediaType);
   }

   private byte[] getBytes(String s) {
      return s.getBytes(StandardCharsets.UTF_8);
   }

   private static String translateType(TestWriteOperation op) {
      switch (op) {
         case PUT_CREATE:
         case PUT_CREATE_FUNCTIONAL:
         case PUT_IF_ABSENT:
         case PUT_MAP_CREATE:
            return "org.infinispan.entry.created";
//         case CACHE_ENTRY_EVICTED:
//            return "org.infinispan.entry.evicted";
//         case CACHE_ENTRY_EXPIRED:
//            return "org.infinispan.entry.expired";
         case PUT_OVERWRITE:
         case PUT_OVERWRITE_FUNCTIONAL:
         case REPLACE:
         case REPLACE_EXACT:
         case REPLACE_FUNCTIONAL:
         case REPLACE_EXACT_FUNCTIONAL:
         case REPLACE_META_FUNCTIONAL:
            return "org.infinispan.entry.modified";
         case REMOVE:
         case REMOVE_EXACT:
         case REMOVE_FUNCTIONAL:
         case REMOVE_EXACT_FUNCTIONAL:
            return "org.infinispan.entry.removed";
         default:
            throw new IllegalArgumentException("Unsupported event type: " + op);
      }
   }
}
