package org.infinispan.client.hotrod.transcoding;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JBOSS_MARSHALLING;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createServerModeCacheManager;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.event.EventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.RawStaticFilteredEventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.StaticFilteredEventLogListener;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.infinispan.commons.marshall.IdentityMarshaller;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;

/**
 * Tests for the Hot Rod client using multiple data formats when interacting with the server.
 *
 * @since 9.3
 */
@Test(groups = "functional", testName = "client.hotrod.transcoding.DataFormatTest")
public class DataFormatTest extends SingleHotRodServerTest {

   private static final String CACHE_NAME = "test";
   private RemoteCache<Object, Object> remoteCache;

   protected ConfigurationBuilder buildCacheConfig() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.encoding().key().mediaType(MediaType.APPLICATION_JBOSS_MARSHALLING_TYPE);
      builder.encoding().value().mediaType(MediaType.APPLICATION_JBOSS_MARSHALLING_TYPE);
      return builder;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = createServerModeCacheManager(hotRodCacheConfiguration());
      ConfigurationBuilder builder = buildCacheConfig();

      cacheManager.defineConfiguration(CACHE_NAME, hotRodCacheConfiguration(builder).build());
      return cacheManager;
   }

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cacheManager, new HotRodServerConfigurationBuilder());
      server.addCacheEventFilterFactory("static-filter-factory", new EventLogListener.StaticCacheEventFilterFactory(42));
      server.addCacheEventFilterFactory("raw-static-filter-factory", new EventLogListener.RawStaticCacheEventFilterFactory());
      return server;
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotrodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache(CACHE_NAME);
   }

   @Test
   public void testValueInMultipleFormats() throws Exception {
      remoteCache.clear();
      String quote = "I find your lack of faith disturbing";

      byte[] jbossMarshalledQuote = marshall(quote);

      // Write to the cache using the default marshaller
      remoteCache.put(1, quote);

      // Read it back as raw bytes using the same key
      Object asBinary = remoteCache.withDataFormat(DataFormat.builder().valueType(APPLICATION_JBOSS_MARSHALLING).valueMarshaller(IdentityMarshaller.INSTANCE).build()).get(1);

      assertArrayEquals(((byte[]) asBinary), jbossMarshalledQuote);

      // Read it back as UTF-8 byte[] using the same key
      Object asUTF8 = remoteCache
            .withDataFormat(DataFormat.builder().valueType(TEXT_PLAIN).valueMarshaller(IdentityMarshaller.INSTANCE).build())
            .get(1);

      assertArrayEquals(quote.getBytes(UTF_8), (byte[]) asUTF8);

      // Read it back as String using the default marshaller for text/plain
      Object asString = remoteCache.withDataFormat(DataFormat.builder().valueType(TEXT_PLAIN).build()).get(1);

      assertEquals(quote, asString);

      // Same, but with metadata
      MetadataValue<Object> metadataValue = remoteCache
            .withDataFormat(DataFormat.builder().valueType(TEXT_PLAIN).build())
            .getWithMetadata(1);

      assertEquals(quote, metadataValue.getValue());

      // Get all entries avoiding de-serialization
      Map<Object, Object> allEntries = remoteCache
            .withDataFormat(DataFormat.builder().valueMarshaller(IdentityMarshaller.INSTANCE).build())
            .getAll(new HashSet<>(Collections.singletonList(1)));

      assertArrayEquals(jbossMarshalledQuote, (byte[]) allEntries.get(1));

      // Read value as JSON in the byte[] form, using the same key
      Object asJSon = remoteCache.withDataFormat(DataFormat.builder().valueType(APPLICATION_JSON).build()).get(1);
      assertArrayEquals(("\"" + quote + "\"").getBytes(UTF_8), (byte[]) asJSon);

      // Read value as JSON in as JsonNode objects
      Object asJSonNode = remoteCache
            .withDataFormat(DataFormat.builder().valueType(APPLICATION_JSON).valueMarshaller(new JacksonMarshaller()).build())
            .get(1);

      assertEquals(new TextNode(quote), asJSonNode);

      // Iterate values without unmarshalling
      Object raw = remoteCache
            .withDataFormat(DataFormat.builder().valueType(APPLICATION_JBOSS_MARSHALLING).valueMarshaller(IdentityMarshaller.INSTANCE).build())
            .values().iterator().next();
      assertArrayEquals(((byte[]) raw), jbossMarshalledQuote);

      // Iterate values converting to JsonNode objects
      Object jsonNode = remoteCache
            .withDataFormat(DataFormat.builder().valueType(APPLICATION_JSON).valueMarshaller(new JacksonMarshaller()).build())
            .values().iterator().next();
      assertEquals(jsonNode, new TextNode(quote));
   }

   @Test
   public void testKeysInMultipleFormats() throws Exception {
      remoteCache.clear();
      cacheManager.getClassWhiteList().addRegexps(".*SocketAddress");
      InetSocketAddress value = InetSocketAddress.createUnresolved("infinispan.org", 8080);

      // Write using String using default Marshaller
      remoteCache.put("1", value);
      assertEquals(value, remoteCache.get("1"));

      // Use UTF-8 key directly as byte[], bypassing the marshaller.
      remoteCache.withDataFormat(DataFormat.builder().keyType(TEXT_PLAIN).keyMarshaller(IdentityMarshaller.INSTANCE).build())
            .put("utf-key".getBytes(), value);

      assertEquals(value, remoteCache.get("utf-key"));

      // Use UTF-8 key with the default UTF8Marshaller
      RemoteCache<Object, Object> remoteCacheUTFKey = this.remoteCache.withDataFormat(DataFormat.builder().keyType(TEXT_PLAIN).build());

      remoteCache.put("temp-key", value);
      assertTrue(remoteCacheUTFKey.containsKey("temp-key"));
      remoteCacheUTFKey.remove("temp-key");
      assertFalse(remoteCacheUTFKey.containsKey("temp-key"));

      assertEquals(value, remoteCacheUTFKey.get("1"));

      // Read value as UTF-8 using a UTF-8 key
      Object asString = this.remoteCache
            .withDataFormat(DataFormat.builder().keyType(TEXT_PLAIN).valueType(TEXT_PLAIN).build())
            .get("1");
      assertEquals(asString, "infinispan.org:8080");

      // Write using manually marshalled values
      remoteCache.withDataFormat(DataFormat.builder()
            .keyType(APPLICATION_JBOSS_MARSHALLING).keyMarshaller(IdentityMarshaller.INSTANCE)
            .valueType(APPLICATION_JBOSS_MARSHALLING).valueMarshaller(IdentityMarshaller.INSTANCE)
            .build())
            .put(marshall(1024), marshall(value));

      assertEquals(value, this.remoteCache.get(1024));

      // Remove using UTF-8 values
      boolean removed = this.remoteCache
            .withDataFormat(DataFormat.builder().valueType(TEXT_PLAIN).build())
            .remove(1024, "wrong-address.com");
      assertFalse(removed);

      removed = this.remoteCache
            .withDataFormat(DataFormat.builder().valueType(TEXT_PLAIN).build())
            .remove(1024, "infinispan.org:8080");
      assertTrue(removed);
      assertFalse(this.remoteCache.containsKey(1024));

   }

   @Test
   public void testBatchOperations() {
      remoteCache.clear();

      cacheManager.getClassWhiteList().addClasses(ComplexKey.class);

      Map<ComplexKey, String> entries = new HashMap<>();
      IntStream.range(0, 50).forEach(i -> {
         ComplexKey key = new ComplexKey(String.valueOf(i), (float) i);
         entries.put(key, UUID.randomUUID().toString());
      });
      remoteCache.putAll(entries);

      // Read all keys as JSON Strings
      RemoteCache<String, String> jsonCache = this.remoteCache.withDataFormat(DataFormat.builder()
            .keyType(APPLICATION_JSON).keyMarshaller(new UTF8StringMarshaller()).build());

      Set<String> jsonKeys = new HashSet<>(jsonCache.keySet());
      jsonKeys.forEach(k -> assertTrue(k.contains("\"_type\":\"org.infinispan.client.hotrod.transcoding.ComplexKey\"")));


      Map<String, String> newEntries = new HashMap<>();

      // Write using JSON
      IntStream.range(50, 100).forEach(i -> {
         String jsonKey = "{\"_type\":\"org.infinispan.client.hotrod.transcoding.ComplexKey\",\"id\":\"" + i + "\",\"ratio\":" + i + "}";
         newEntries.put(jsonKey, UUID.randomUUID().toString());
      });
      jsonCache.putAll(newEntries);

      // Read it back as regular objects
      Set<ComplexKey> keys = new HashSet<>();
      IntStream.range(60, 70).forEach(i -> keys.add(new ComplexKey(String.valueOf(i), (float) i)));
      Set<ComplexKey> returned = remoteCache.getAll(keys).keySet().stream().map(ComplexKey.class::cast).collect(Collectors.toSet());
      assertEquals(keys, returned);
   }

   @Test
   public void testListenersWithDifferentFormats() {
      remoteCache.clear();

      ComplexKey complexKey = new ComplexKey("Key-1", 89.88f);

      // Receive events as JSON Strings
      DataFormat jsonStringFormat = DataFormat.builder().keyType(APPLICATION_JSON).keyMarshaller(new UTF8StringMarshaller()).build();

      EventLogListener<Object> l = new EventLogListener<>(remoteCache.withDataFormat(jsonStringFormat));

      withClientListener(l, remote -> {
         remoteCache.put(complexKey, UUID.randomUUID());
         l.expectOnlyCreatedEvent("{\"_type\":\"org.infinispan.client.hotrod.transcoding.ComplexKey\",\"id\":\"Key-1\",\"ratio\":89.88}");
      });
   }

   @Test
   public void testNonRawFilteredListeners() {
      remoteCache.clear();
      RemoteCache<Integer, String> remoteCache = this.remoteCache.withDataFormat(DataFormat.builder().valueType(TEXT_PLAIN).build());
      StaticFilteredEventLogListener<Integer> l = new StaticFilteredEventLogListener<>(remoteCache);
      withClientListener(l, remote -> {
         remoteCache.put(1, "value1");
         l.expectNoEvents();
         remoteCache.put(42, "value2");
         l.expectOnlyCreatedEvent(42);
      });
   }

   @Test
   public void testRawFilteredListeners() {
      remoteCache.clear();

      RemoteCache<Object, Object> jsonCache = this.remoteCache
            .withDataFormat(DataFormat.builder().keyType(APPLICATION_JSON).keyMarshaller(new UTF8StringMarshaller()).build());

      RawStaticFilteredEventLogListener<Object> l = new RawStaticFilteredEventLogListener<>(jsonCache);

      withClientListener(l, remote -> {
         jsonCache.put("1", UUID.randomUUID());
         l.expectNoEvents();
         jsonCache.put("2", UUID.randomUUID());
         l.expectOnlyCreatedEvent("2");
      });
   }

   @Test
   public void testJsonFromDefaultCache() {
      DataFormat jsonValues = DataFormat.builder()
            .valueType(APPLICATION_JSON)
            .valueMarshaller(new UTF8StringMarshaller()).build();

      RemoteCache<Integer, String> cache = remoteCacheManager.getCache().withDataFormat(jsonValues);

      String value = "{\"json_key\":\"json_value\"}";
      cache.put(1, value);

      String valueAsJson = cache.get(1);

      assertEquals(value, valueAsJson);
   }

   private byte[] marshall(Object o) throws Exception {
      return remoteCache.getRemoteCacheManager().getMarshaller().objectToByteBuffer(o);
   }
}

class JacksonMarshaller extends AbstractMarshaller {

   private static final ObjectMapper MAPPER = new ObjectMapper();

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException {
      byte[] bytes = MAPPER.writeValueAsBytes(o);
      return new ByteBufferImpl(bytes, 0, bytes.length);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException {
      return MAPPER.readTree(buf);
   }

   @Override
   public boolean isMarshallable(Object o) {
      return true;
   }

   @Override
   public MediaType mediaType() {
      return APPLICATION_JSON;
   }
}

@SuppressWarnings("unused")
class ComplexKey implements Serializable {
   public ComplexKey() {
   }

   private String id;

   private Float ratio;

   ComplexKey(String id, Float ratio) {
      this.id = id;
      this.ratio = ratio;
   }

   public void setId(String id) {
      this.id = id;
   }

   public void setRatio(Float ratio) {
      this.ratio = ratio;
   }

   public String getId() {
      return id;
   }

   public Float getRatio() {
      return ratio;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ComplexKey that = (ComplexKey) o;
      return Objects.equals(id, that.id) &&
            Objects.equals(ratio, that.ratio);
   }

   @Override
   public int hashCode() {

      return Objects.hash(id, ratio);
   }
}
