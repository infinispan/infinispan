package org.infinispan.dataconversion;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.notifications.Listener.Observation.POST;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.dataconversion.CompatModeEncoder;
import org.infinispan.commons.dataconversion.GenericJbossMarshallerEncoder;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.JavaSerializationEncoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.UTF8Encoder;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.ContentTypeConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.context.InvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Test various data conversion scenarios.
 *
 * @since 9.1
 */
@Test(groups = "functional", testName = "core.DataConversionTest")
public class DataConversionTest extends AbstractInfinispanTest {

   @Test
   public void testReadUnencoded() throws Exception {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.memory().storageType(StorageType.OFF_HEAP);

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(cfg)) {
         @Override
         public void call() throws IOException, InterruptedException {
            Cache<String, Person> cache = cm.getCache();

            Marshaller marshaller = cache.getAdvancedCache().getComponentRegistry().getCacheMarshaller();

            Person value = new Person();
            cache.put("1", value);

            // Read using default valueEncoder
            assertEquals(cache.get("1"), value);

            // Read unencoded
            Cache<?, ?> unencodedCache = cache.getAdvancedCache().withEncoding(IdentityEncoder.class);
            assertEquals(unencodedCache.get(marshaller.objectToByteBuffer("1")), marshaller.objectToByteBuffer(value));
         }
      });
   }

   @Test
   public void testUTF8Encoders() throws Exception {
      ConfigurationBuilder cfg = new ConfigurationBuilder();

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(cfg)) {

         String charset = "UTF-8";

         private byte[] asUTF8Bytes(String value) throws UnsupportedEncodingException {
            return value.getBytes(charset);
         }

         @Override
         public void call() throws IOException, InterruptedException {
            Cache<byte[], byte[]> cache = cm.getCache();

            String keyUnencoded = "1";
            String valueUnencoded = "value";
            cache.put(asUTF8Bytes(keyUnencoded), asUTF8Bytes(valueUnencoded));

            // Read using different valueEncoder
            Cache utf8Cache = cache.getAdvancedCache().withEncoding(UTF8Encoder.class);
            assertEquals(utf8Cache.get(keyUnencoded), valueUnencoded);

            // Write with one valueEncoder and read with another
            String key2Unencoded = "2";
            String value2Unencoded = "anotherValue";
            utf8Cache.put(key2Unencoded, value2Unencoded);

            assertEquals(cache.get(asUTF8Bytes(key2Unencoded)), asUTF8Bytes(value2Unencoded));
         }
      });
   }

   @Test
   public void testObjectEncoder() throws Exception {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(new ConfigurationBuilder())) {

         private byte[] marshall(Object o) throws IOException, InterruptedException {
            return (byte[]) GenericJbossMarshallerEncoder.INSTANCE.toStorage(o);
         }

         @Override
         public void call() throws IOException, InterruptedException {
            Cache<byte[], byte[]> cache = cm.getCache();

            // Write encoded content to the cache
            Person key1 = new Person("key1");
            Person value1 = new Person("value1");
            byte[] encodedKey1 = marshall(key1);
            byte[] encodedValue1 = marshall(value1);
            cache.put(encodedKey1, encodedValue1);

            // Read encoded content
            assertEquals(cache.get(encodedKey1), encodedValue1);

            // Read with a different valueEncoder
            AdvancedCache<Person, Person> encodingCache = (AdvancedCache<Person, Person>) cache.getAdvancedCache().withEncoding(GenericJbossMarshallerEncoder.class);

            assertEquals(encodingCache.get(key1), value1);
         }
      });

   }

   @Test
   public void testCompatModeEncoder() throws Exception {
      ConfigurationBuilder cfg = new ConfigurationBuilder();

      JavaSerializationMarshaller marshaller = new JavaSerializationMarshaller();

      cfg.compatibility().marshaller(marshaller).enable();

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createServerModeCacheManager(cfg)) {

         private byte[] marshall(Object o) throws IOException, InterruptedException {
            return marshaller.objectToByteBuffer(o);
         }

         @Override
         public void call() throws IOException, InterruptedException {
            Cache<byte[], byte[]> cache = cm.getCache();

            Cache c = cache.getAdvancedCache().withEncoding(CompatModeEncoder.class);

            // Write encoded content to the cache
            int key1 = 2017;
            Person value1 = new Person();
            byte[] encodedKey = marshall(key1);
            byte[] encodedValue = marshall(value1);
            c.put(encodedKey, encodedValue);

            // Read encoded content
            assertEquals(c.get(encodedKey), encodedValue);

            // Read without encoding
            Cache noEncodingCache = cache.getAdvancedCache().withEncoding(IdentityEncoder.class);
            assertEquals(noEncodingCache.get(key1), value1);

            // Write unencoded content and read encoded
            int key2 = 2019;
            Person value2 = new Person("another");
            noEncodingCache.put(key2, value2);


            assertEquals(c.get(marshall(key2)), marshall(value2));
         }
      });
   }

   @Test
   public void testExtractIndexable() throws Exception {
      ConfigurationBuilder cfg = new ConfigurationBuilder();

      cfg.customInterceptors().addInterceptor().after(EntryWrappingInterceptor.class).interceptor(new TestInterceptor(1));

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(cfg)) {

         @Override
         public void call() throws IOException, InterruptedException {
            ConfigurationBuilder offHeapConfig = new ConfigurationBuilder();
            offHeapConfig.memory().storageType(StorageType.OFF_HEAP);
            offHeapConfig.customInterceptors().addInterceptor().after(EntryWrappingInterceptor.class).interceptor(new TestInterceptor(1));

            ConfigurationBuilder compatConfig = new ConfigurationBuilder();
            compatConfig.compatibility().enable().marshaller(new JavaSerializationMarshaller());
            compatConfig.customInterceptors().addInterceptor().after(EntryWrappingInterceptor.class).interceptor(new TestInterceptor(1));

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

   private class TestInterceptor extends BaseCustomAsyncInterceptor {

      private final int i;
      private DataConversion valueDataConversion;

      TestInterceptor(int i) {
         this.i = i;
      }

      @Inject
      protected void injectDependencies(Cache<?, ?> cache) {
         this.valueDataConversion = cache.getAdvancedCache().getValueDataConversion();
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
            throws Throwable {

         assertNotNull(valueDataConversion);
         Object value = command.getValue();
         assertEquals(i, valueDataConversion.fromStorage(value));
         return invokeNext(ctx, command);
      }
   }

   @Listener(observation = POST)
   private static class SimpleListener {

      private List<CacheEntryEvent> events = new ArrayList<>();

      @CacheEntryCreated
      public void cacheEntryCreated(CacheEntryEvent ev) {
         events.add(ev);
      }

   }

   @Test
   public void testConversionWithListeners() throws Exception {
      ConfigurationBuilder cfg = new ConfigurationBuilder();

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(cfg)) {
         @Override
         public void call() throws IOException, InterruptedException {
            Cache<String, Person> cache = cm.getCache();
            // Obtain cache with custom valueEncoder
            Cache storeMarshalled = cache.getAdvancedCache().withEncoding(JavaSerializationEncoder.class);

            // Add a listener
            SimpleListener simpleListener = new SimpleListener();
            storeMarshalled.addListener(simpleListener);

            Person value = new Person();
            storeMarshalled.put("1", value);

            // Assert values returned are passed through the valueEncoder
            assertEquals(simpleListener.events.size(), 1);
            assertEquals(simpleListener.events.get(0).getKey(), "1");
            assertEquals(simpleListener.events.get(0).getValue(), value);
         }
      });
   }

   @Test
   @SuppressWarnings("unchecked")
   public void testTranscoding() throws Exception {

      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      cfg.encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(cfg)) {
         @Override
         public void call() throws IOException, InterruptedException {
            Cache<String, Map<String, String>> cache = cm.getCache();

            EncoderRegistry encoderRegistry = cache.getAdvancedCache().getComponentRegistry().getComponent(EncoderRegistry.class);
            encoderRegistry.registerTranscoder(new ObjectXMLTranscoder());

            // Store a map in the cache
            Map<String, String> valueMap = new HashMap<>();
            valueMap.put("BTC", "Bitcoin");
            valueMap.put("ETH", "Ethereum");
            valueMap.put("LTC", "Litecoin");
            cache.put("CoinMap", valueMap);

            assertEquals(valueMap, cache.get("CoinMap"));

            // Obtain the value with a different MediaType
            AdvancedCache<String, String> xmlCache = (AdvancedCache<String, String>) cache.getAdvancedCache()
                  .withMediaType(APPLICATION_OBJECT_TYPE, APPLICATION_XML_TYPE);

            assertEquals(xmlCache.get("CoinMap"), "<root><BTC>Bitcoin</BTC><ETH>Ethereum</ETH><LTC>Litecoin</LTC></root>");

            // Reading with same configured MediaType should not change content
            assertEquals(xmlCache.withMediaType(APPLICATION_OBJECT_TYPE, APPLICATION_OBJECT_TYPE).get("CoinMap"), valueMap);

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
   public void testTranscodingWithCustomConfig() throws Exception {

      ConfigurationBuilder cfg = new ConfigurationBuilder();

      ContentTypeConfigurationBuilder key = cfg.encoding().key();
      ContentTypeConfigurationBuilder value = cfg.encoding().value();

      key.mediaType("application/foo");
      value.mediaType("application/bar");

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(cfg)) {
         @Override
         public void call() throws IOException, InterruptedException {
            Cache<String, String> cache = cm.getCache();

            EncoderRegistry encoderRegistry = cache.getAdvancedCache().getComponentRegistry().getComponent(EncoderRegistry.class);
            encoderRegistry.registerTranscoder(new FooBarTranscoder());

            cache.put("foo-key", "bar-value");
            assertEquals(cache.get("foo-key"), "bar-value");

            Cache<String, String> fooCache = (Cache<String, String>) cache.getAdvancedCache().withMediaType("application/foo", "application/foo");
            assertEquals(fooCache.get("foo-key"), "foo-value");

            Cache<String, String> barCache = (Cache<String, String>) cache.getAdvancedCache().withMediaType("application/bar", "application/bar");
            assertEquals(barCache.get("bar-key"), "bar-value");

            Cache<String, String> barFooCache = (Cache<String, String>) cache.getAdvancedCache().withMediaType("application/bar", "application/foo");
            assertEquals(barFooCache.get("bar-key"), "foo-value");
         }
      });
   }

   @Test
   @SuppressWarnings("unchecked")
   public void testTextTranscoder() throws Exception {

      ConfigurationBuilder cfg = new ConfigurationBuilder();

      cfg.encoding().key().mediaType("text/plain; charset=ISO-8859-1");
      cfg.encoding().value().mediaType("text/plain; charset=UTF-8");

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(cfg)) {
         @Override
         public void call() throws IOException, InterruptedException {
            Cache<byte[], byte[]> cache = cm.getCache();

            byte[] key = "key1".getBytes(ISO_8859_1);
            byte[] value = new byte[]{97, 118, 105, -61, -93, 111};  // 'avião' in UTF-8
            cache.put(key, value);

            assertEquals(cache.get(key), value);

            // Value as UTF-16
            Cache<byte[], byte[]> utf16ValueCache = (Cache<byte[], byte[]>) cache.getAdvancedCache().withMediaType("text/plain; charset=ISO-8859-1", "text/plain; charset=UTF-16");

            assertEquals(utf16ValueCache.get(key), new byte[]{-2, -1, 0, 97, 0, 118, 0, 105, 0, -29, 0, 111});
         }
      });
   }
}
