package org.infinispan.it.endpoints;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.util.concurrent.CompletionStages.join;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test embedded caches, Hot Rod, and REST endpoints.
 * In the case (very common) that the cache is protobuf-encoded.
 */
@Test(groups = "functional", testName = "it.endpoints.ProtoCacheRestHotRodTest")
public class ProtoCacheRestHotRodTest extends AbstractInfinispanTest {

   private static final DateFormat dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);

   EndpointsCacheFactory<String, Object> cacheFactory;

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory = new EndpointsCacheFactory.Builder<String, Object>().withCacheMode(CacheMode.LOCAL)
            .withContextInitializer(EndpointITSCI.INSTANCE).withMediaType(MediaType.APPLICATION_PROTOSTREAM).build();
      dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
   }

   @AfterClass
   protected void teardown() {
      EndpointsCacheFactory.killCacheFactories(cacheFactory);
   }

   public void testCustomObjectHotRodPutRestGetAcceptJSONAndXML_listOfStrings_asRootValue() {
      String key = "bla";
      List<String> value = new ArrayList<>();
      for (int i=0; i<3; i++) {
         value.add("ciao");
      }

      RemoteCache<String, Object> remote = cacheFactory.getHotRodCache();
      assertNull(remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, value));

      // 2. Get with REST (accept application/json)
      RestResponse response = join(cacheFactory.getRestCacheClient().get(key, APPLICATION_JSON_TYPE));
      assertEquals(200, response.status());
      String body = response.body();
      Json json = Json.read(body);
      assertThat(json.at("_value").asJsonList())
            .extracting(j -> j.at("string").asString()).containsExactly("ciao", "ciao", "ciao");

      // 3. Entries with REST (with metadata)
      response = join(cacheFactory.getRestCacheClient().entries(10, true));
      assertEquals(200, response.status());
      body = response.body();
      json = Json.read(body).asJsonList().get(0).at("value");
      assertThat(json.at("_value").asJsonList())
            .extracting(j -> j.at("string").asString()).containsExactly("ciao", "ciao", "ciao");

      // 4. Entries with REST (without metadata)
      response = join(cacheFactory.getRestCacheClient().entries(10, false));
      assertEquals(200, response.status());
      body = response.body();
      json = Json.read(body).asJsonList().get(0).at("value");
      assertThat(json.at("_value").asJsonList())
            .extracting(j -> j.at("string").asString()).containsExactly("ciao", "ciao", "ciao");
   }
}
