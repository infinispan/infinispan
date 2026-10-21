package org.infinispan.server.functional.rest;

import static org.infinispan.server.test.core.Common.sync;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.Base16Codec;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.jupiter.InfinispanServer;
import org.junit.jupiter.api.Test;

public class RestStreamingUnconvertible {

   @InfinispanServer(ClusteredIT.class)
   public static TestClientDriver SERVERS;

   /** A value whose writeObject() skips defaultWriteObject() and writes a nested object with its own writeObject() before primitive data (the Jetty SessionData-like layout from the issue). */
   public static class NoDefaultWriteObjectValue implements Serializable {
      Object a;
      String b;

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.writeObject(new java.util.ArrayList<>(java.util.List.of("x")));
         out.writeInt(7);
      }
   }

   /** A plain POJO that serializes normally. */
   public static class PlainValue implements Serializable {
      int v;

      @Override
      public String toString() {
         return "{\"v\":" + v + "}";
      }
   }

   private RestCacheClient createSerializedObjectCache(String name) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.encoding().value().mediaType(MediaType.APPLICATION_SERIALIZED_OBJECT.toString());
      RestClient client = SERVERS.rest().get();
      RestCacheClient cache = client.cache(name);
      sync(cache.createWithConfiguration(RestEntity.create(MediaType.APPLICATION_JSON, Common.cacheConfigToJson(name, builder.build()))));
      return cache;
   }

   @Test
   public void testStreamEntriesWithUnconvertibleValueFallsBackToHex() {
      RestCacheClient cache = createSerializedObjectCache(SERVERS.getMethodName());

      // Put a good entry and a bad entry (garbage bytes that look like a serialized object header but fail to deserialize)
      PlainValue good = new PlainValue();
      good.v = 42;
      byte[] goodBytes = serialize(good);
      byte[] badBytes = {(byte) 0xAC, (byte) 0xED, 0x00, 0x05, (byte) 0xFF};

      sync(cache.put("good", RestEntity.create(MediaType.APPLICATION_SERIALIZED_OBJECT, goodBytes)));
      sync(cache.put("bad", RestEntity.create(MediaType.APPLICATION_SERIALIZED_OBJECT, badBytes)));

      // Stream entries: the request must succeed (200 OK) and return both entries.
      // The values are negotiated to text/plain; since the server's class allow list does not include
      // PlainValue, both values fail to transcode and fall back to the hex representation of the raw bytes.
      // The critical assertion is that the stream stays alive and returns both entries.
      RestResponse response = sync(cache.entries(true));
      String body = response.body();
      response.close();
      assertEquals(200, response.status(), body);
      List<Json> entries = Json.read(body).asJsonList();
      assertEquals(2, entries.size());
      String goodKeyHex = Base16Codec.encode("good".getBytes(StandardCharsets.UTF_8));
      String badKeyHex = Base16Codec.encode("bad".getBytes(StandardCharsets.UTF_8));
      String goodValueHex = Base16Codec.encode(goodBytes);
      String badValueHex = Base16Codec.encode(badBytes);
      boolean foundGood = false, foundBad = false;
      for (Json entry : entries) {
         String key = entry.at("key").asString();
         if (key.equals(goodKeyHex)) {
            foundGood = true;
            // The value must be the full hex rendering of the raw content, prefix included: a fallback returning
            // only the 0x prefix would otherwise pass
            assertEquals(goodValueHex, entry.at("value").asString(), "good entry value");
         } else if (key.equals(badKeyHex)) {
            foundBad = true;
            assertEquals(badValueHex, entry.at("value").asString(), "bad entry value");
         }
      }
      assertTrue(foundGood, "should find the good entry: " + body);
      assertTrue(foundBad, "should find the bad entry: " + body);
   }

   @Test
   public void testGetEntryWithNoDefaultWriteObjectValueRendersEmptyObject() {
      RestCacheClient cache = createSerializedObjectCache(SERVERS.getMethodName());

      // Put a value whose writeObject() skips defaultWriteObject()
      byte[] valueBytes = serialize(new NoDefaultWriteObjectValue());
      sync(cache.put("k", RestEntity.create(MediaType.APPLICATION_SERIALIZED_OBJECT, valueBytes)));

      // GET with Accept: application/json — the value should render as an empty JSON object {}
      RestResponse response = sync(cache.get("k", MediaType.APPLICATION_JSON_TYPE));
      String body = response.body().trim();
      response.close();
      assertEquals(200, response.status(), body);
      assertEquals("{}", body, "no-defaultWriteObject value should render as empty JSON object");
   }

   @Test
   public void testGetEntryWithUnconvertibleValueReturnsConversionError() {
      RestCacheClient cache = createSerializedObjectCache(SERVERS.getMethodName());

      // a valid stream header followed by an invalid type code: the value cannot be converted
      byte[] badBytes = {(byte) 0xAC, (byte) 0xED, 0x00, 0x05, (byte) 0xFF};
      sync(cache.put("bad", RestEntity.create(MediaType.APPLICATION_SERIALIZED_OBJECT, badBytes)));

      // GET with Accept: application/json — a single entry that cannot be converted returns an entry-level
      // conversion error response rather than aborting a stream or logging an error
      RestResponse response = sync(cache.get("bad", MediaType.APPLICATION_JSON_TYPE));
      String body = response.body();
      response.close();
      assertEquals(400, response.status(), body);
   }

   private static byte[] serialize(Object obj) {
      try {
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         try (ObjectOutputStream os = new ObjectOutputStream(baos)) {
            os.writeObject(obj);
         }
         return baos.toByteArray();
      } catch (IOException e) {
         throw new IllegalStateException("Failed to serialize", e);
      }
   }
}
