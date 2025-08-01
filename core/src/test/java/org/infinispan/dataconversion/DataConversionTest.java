package org.infinispan.dataconversion;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.notifications.Listener.Observation.POST;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.infinispan.test.fwk.TestCacheManagerFactory.DEFAULT_CACHE_NAME;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Test various data conversion scenarios.
 *
 * @since 9.1
 */
@Test(groups = "functional", testName = "core.DataConversionTest")
@SuppressWarnings("unchecked")
public class DataConversionTest extends AbstractInfinispanTest {

   @Test
   public void testReadUnencoded() {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.memory().storage(StorageType.OFF_HEAP);

      withCacheManager(new CacheManagerCallable(
            createCacheManager(TestDataSCI.INSTANCE, cfg)) {

         private final EncoderRegistry registry = TestingUtil.extractGlobalComponent(cm, EncoderRegistry.class);

         public Object asStored(Object object) {
            return registry.convert(object, APPLICATION_OBJECT, APPLICATION_PROTOSTREAM);
         }

         @Override
         public void call() {
            cm.getClassAllowList().addClasses(Person.class);
            Cache<String, Person> cache = cm.getCache();

            Person value = new Person();
            cache.put("1", value);

            // Read using default valueEncoder
            assertEquals(cache.get("1"), value);

            // Read unencoded
            Cache<?, ?> unencodedCache = cache.getAdvancedCache().withStorageMediaType();
            assertEquals(unencodedCache.get(asStored("1")), asStored(value));
         }
      });
   }

   @Test
   public void testExtractIndexable() {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder().nonClusteredDefault();
      TestCacheManagerFactory.addInterceptor(global, DEFAULT_CACHE_NAME::equals, new TestInterceptor(1), TestCacheManagerFactory.InterceptorPosition.AFTER, EntryWrappingInterceptor.class);
      TestCacheManagerFactory.addInterceptor(global, "offheap"::equals, new TestInterceptor(1), TestCacheManagerFactory.InterceptorPosition.AFTER, EntryWrappingInterceptor.class);
      TestCacheManagerFactory.addInterceptor(global, "compat"::equals, new TestInterceptor(1), TestCacheManagerFactory.InterceptorPosition.AFTER, EntryWrappingInterceptor.class);
      withCacheManager(new CacheManagerCallable(
            createCacheManager(TestDataSCI.INSTANCE, new ConfigurationBuilder())) {

         @Override
         public void call() {
            ConfigurationBuilder offHeapConfig = new ConfigurationBuilder();
            offHeapConfig.memory().storage(StorageType.OFF_HEAP);

            ConfigurationBuilder compatConfig = new ConfigurationBuilder();

            cm.defineConfiguration("offheap", offHeapConfig.build());
            cm.defineConfiguration("compat", compatConfig.build());

            Cache<Object, Object> cache = cm.getCache();
            Cache<Object, Object> offheapCache = cm.getCache("offheap");
            Cache<Object, Object> compatCache = cm.getCache("compat");
            cache.put(1, 1);
            offheapCache.put(1, 1);
            compatCache.put(1, 1);

            assertEquals(1, cache.get(1));
            assertEquals(1, offheapCache.get(1));
            assertEquals(1, compatCache.get(1));
         }
      });
   }

   @Test
   @SuppressWarnings("unchecked")
   public void testTranscoding() {

      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      cfg.encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);

      withCacheManager(new CacheManagerCallable(
            createCacheManager(TestDataSCI.INSTANCE, cfg)) {
         @Override
         public void call() {
            Cache<String, Map<String, String>> cache = cm.getCache();

            EncoderRegistry encoderRegistry = TestingUtil.extractGlobalComponent(cm, EncoderRegistry.class);
            encoderRegistry.registerTranscoder(new ObjectXMLTranscoder());

            // Store a map in the cache
            Map<String, String> valueMap = new HashMap<>();
            valueMap.put("BTC", "Bitcoin");
            valueMap.put("ETH", "Ethereum");
            valueMap.put("LTC", "Litecoin");
            cache.put("CoinMap", valueMap);

            assertEquals(valueMap, cache.get("CoinMap"));

            // Obtain the value with a different MediaType
            AdvancedCache<String, String> xmlCache = cache.getAdvancedCache().withMediaType(APPLICATION_OBJECT, APPLICATION_XML);

            assertEquals(xmlCache.get("CoinMap"), "<root><BTC>Bitcoin</BTC><ETH>Ethereum</ETH><LTC>Litecoin</LTC></root>");

            // Reading with same configured MediaType should not change content
            assertEquals(xmlCache.withMediaType(APPLICATION_OBJECT, APPLICATION_OBJECT).get("CoinMap"), valueMap);

            // Writing using XML
            xmlCache.put("AltCoinMap", "<root><CAT>Catcoin</CAT><DOGE>Dogecoin</DOGE></root>");

            // Read using object from undecorated cache
            Map<String, String> map = cache.get("AltCoinMap");

            assertEquals(map.get("CAT"), "Catcoin");
            assertEquals(map.get("DOGE"), "Dogecoin");
         }
      });
   }

   @Test
   @SuppressWarnings("unchecked")
   public void testTranscodingWithCustomConfig() {
      withCacheManager(new CacheManagerCallable(createCacheManager(TestDataSCI.INSTANCE)) {
         @Override
         public void call() {
            EncoderRegistry encoderRegistry = TestingUtil.extractGlobalComponent(cm, EncoderRegistry.class);
            encoderRegistry.registerTranscoder(new FooBarTranscoder());
            ConfigurationBuilder cfg = new ConfigurationBuilder();
            cfg.encoding().key().mediaType("application/foo");
            cfg.encoding().value().mediaType("application/bar");
            cm.defineConfiguration("foobar", cfg.build());

            Cache<String, String> cache = cm.getCache("foobar");

            cache.put("foo-key", "bar-value");
            assertEquals(cache.get("foo-key"), "bar-value");

            MediaType appFoo = MediaType.fromString("application/foo");
            MediaType appBar = MediaType.fromString("application/bar");
            Cache<String, String> fooCache = cache.getAdvancedCache().withMediaType(appFoo, appFoo);
            assertEquals(fooCache.get("foo-key"), "foo-value");

            Cache<String, String> barCache = cache.getAdvancedCache().withMediaType(appBar, appBar);
            assertEquals(barCache.get("bar-key"), "bar-value");

            Cache<String, String> barFooCache = cache.getAdvancedCache().withMediaType(appBar, appFoo);
            assertEquals(barFooCache.get("bar-key"), "foo-value");
         }
      });
   }

   @Test
   @SuppressWarnings("unchecked")
   public void testTextTranscoder() {

      ConfigurationBuilder cfg = new ConfigurationBuilder();

      cfg.encoding().key().mediaType("text/plain; charset=ISO-8859-1");
      cfg.encoding().value().mediaType("text/plain; charset=UTF-8");

      withCacheManager(new CacheManagerCallable(
            createCacheManager(TestDataSCI.INSTANCE, cfg)) {
         @Override
         public void call() {
            AdvancedCache<Object, Object> cache = cm.getCache().getAdvancedCache().withStorageMediaType();

            byte[] key = "key1".getBytes(ISO_8859_1);
            byte[] value = new byte[]{97, 118, 105, -61, -93, 111};  // 'avião' in UTF-8
            cache.put(key, value);

            assertEquals(cache.get(key), value);

            // Value as UTF-16
            Cache<byte[], byte[]> utf16ValueCache = cache.getAdvancedCache().withMediaType(MediaType.fromString("text/plain; charset=ISO-8859-1"), MediaType.fromString("text/plain; charset=UTF-16"));

            assertEquals(utf16ValueCache.get(key), new byte[]{-2, -1, 0, 97, 0, 118, 0, 105, 0, -29, 0, 111});
         }
      });
   }

   @Test
   public void testJavaSerialization() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.serialization().marshaller(new JavaSerializationMarshaller());
      try (DefaultCacheManager manager = new DefaultCacheManager(gcb.build())) {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         builder.encoding().mediaType(MediaType.APPLICATION_SERIALIZED_OBJECT_TYPE);

         Cache<String, String> cache = manager.createCache("cache", builder.build());
         cache.put("key", "value");

         JavaSerializationMarshaller marshaller = new JavaSerializationMarshaller();
         DataContainer<?, ?> dataContainer = cache.getAdvancedCache().getDataContainer();
         InternalCacheEntry<?, ?> cacheEntry = dataContainer.peek(new WrappedByteArray(marshaller.objectToByteBuffer("key")));
         assertEquals(new WrappedByteArray(marshaller.objectToByteBuffer("value")), cacheEntry.getValue());
      }
   }

   @SuppressWarnings("unused")
   static class TestInterceptor extends BaseCustomAsyncInterceptor {

      private final int i;

      @Inject ComponentRef<AdvancedCache<?, ?>> cache;

      TestInterceptor(int i) {
         this.i = i;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
            throws Throwable {

         DataConversion valueDataConversion = cache.wired().getValueDataConversion();
         assertNotNull(valueDataConversion);
         Object value = command.getValue();
         assertEquals(i, valueDataConversion.fromStorage(value));
         return invokeNext(ctx, command);
      }
   }

   @Listener(observation = POST)
   @SuppressWarnings("unused")
   private static class SimpleListener {

      private final List<CacheEntryEvent> events = new ArrayList<>();

      @CacheEntryCreated
      public void cacheEntryCreated(CacheEntryEvent ev) {
         events.add(ev);
      }

   }
}
