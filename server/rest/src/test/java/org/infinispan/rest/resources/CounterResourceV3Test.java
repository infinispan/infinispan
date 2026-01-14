package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.ConvertUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

/**
 * Tests for REST v3 Counter API endpoints.
 *
 * Note: The RestCounterClient currently uses v2 endpoints internally.
 * This test class verifies that v3 endpoints work correctly when called directly.
 *
 * @since 16.0
 */
@Test(groups = "functional", testName = "rest.CounterResourceV3Test")
public class CounterResourceV3Test extends AbstractRestResourceTest {

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
      CounterManager counterManager = EmbeddedCounterManagerFactory.asCounterManager(cm);
      counterManager.defineCounter("weak", CounterConfiguration.builder(CounterType.WEAK).build());
      counterManager.defineCounter("strong", CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).build());
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new CounterResourceV3Test().withSecurity(false).browser(false),
            new CounterResourceV3Test().withSecurity(false).browser(true),
            new CounterResourceV3Test().withSecurity(true).browser(false),
            new CounterResourceV3Test().withSecurity(true).browser(true),
      };
   }

   @Test
   public void testWeakCounterLifecycle() {
      String counterName = "v3-sample-counter";
      CounterConfiguration counterConfig = CounterConfiguration.builder(CounterType.WEAK)
            .initialValue(5).storage(Storage.VOLATILE).concurrencyLevel(6).build();
      createCounterV3(counterName, counterConfig);

      // Get configuration using v3 endpoint
      RestResponse response = join(client.raw().get("/rest/v3/counters/" + counterName + "/config", Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));
      assertThat(response).isOk();
      Json jsonNode = Json.read(response.body());
      Json config = jsonNode.at("weak-counter");
      assertEquals(config.at("initial-value").asInteger(), 5);
      assertEquals(config.at("storage").asString(), "VOLATILE");
      assertEquals(config.at("concurrency-level").asInteger(), 6);

      // Delete using v3 endpoint
      response = join(client.raw().delete("/rest/v3/counters/" + counterName));
      assertThat(response).isOk();

      // Verify it's deleted using v3 endpoint
      response = join(client.raw().get("/rest/v3/counters/" + counterName + "/config"));
      assertThat(response).isNotFound();
   }

   @Test
   public void testWeakCounterOps() {
      String name = "v3-weak-test";
      createCounterV3(name, CounterConfiguration.builder(CounterType.WEAK).initialValue(5).build());

      // Increment using v3 endpoint
      RestResponse response = join(client.raw().post("/rest/v3/counters/" + name + "/_increment"));
      assertThat(response).hasNoContent();
      waitForCounterToReachV3(name, 6);

      response = join(client.raw().post("/rest/v3/counters/" + name + "/_increment"));
      assertThat(response).hasNoContent();
      waitForCounterToReachV3(name, 7);

      // Decrement using v3 endpoint
      response = join(client.raw().post("/rest/v3/counters/" + name + "/_decrement"));
      assertThat(response).hasNoContent();
      waitForCounterToReachV3(name, 6);

      response = join(client.raw().post("/rest/v3/counters/" + name + "/_decrement"));
      assertThat(response).hasNoContent();
      waitForCounterToReachV3(name, 5);

      // Add delta using v3 endpoint
      response = join(client.raw().post("/rest/v3/counters/" + name + "/_add?delta=10"));
      assertThat(response).hasNoContent();
      waitForCounterToReachV3(name, 15);

      // Reset using v3 endpoint
      response = join(client.raw().post("/rest/v3/counters/" + name + "/_reset"));
      assertThat(response).hasNoContent();
      waitForCounterToReachV3(name, 5);
   }

   @Test
   public void testStrongCounterOps() {
      String name = "v3-strong-test";
      createCounterV3(name, CounterConfiguration.builder(CounterType.BOUNDED_STRONG).lowerBound(0).upperBound(100)
            .initialValue(0).build());

      // Increment using v3 endpoint
      RestResponse response = join(client.raw().post("/rest/v3/counters/" + name + "/_increment"));
      assertThat(response).hasReturnedText("1");

      response = join(client.raw().post("/rest/v3/counters/" + name + "/_increment"));
      assertThat(response).hasReturnedText("2");

      // Decrement using v3 endpoint
      response = join(client.raw().post("/rest/v3/counters/" + name + "/_decrement"));
      assertThat(response).hasReturnedText("1");

      response = join(client.raw().post("/rest/v3/counters/" + name + "/_decrement"));
      assertThat(response).hasReturnedText("0");

      // Add delta using v3 endpoint
      response = join(client.raw().post("/rest/v3/counters/" + name + "/_add?delta=35"));
      assertThat(response).hasReturnedText("35");
      waitForCounterToReachV3(name, 35);

      // Compare and set using v3 endpoint
      response = join(client.raw().post("/rest/v3/counters/" + name + "/_compare-and-set?expect=5&update=32"));
      assertThat(response).hasReturnedText("false");

      response = join(client.raw().post("/rest/v3/counters/" + name + "/_compare-and-set?expect=35&update=50"));
      assertThat(response).hasReturnedText("true");
      waitForCounterToReachV3(name, 50);

      // Compare and swap using v3 endpoint
      response = join(client.raw().post("/rest/v3/counters/" + name + "/_compare-and-swap?expect=50&update=90"));
      assertThat(response).hasReturnedText("50");

      // Get value using v3 endpoint
      response = join(client.raw().get("/rest/v3/counters/" + name));
      assertThat(response).hasReturnedText("90");

      // Get and set using v3 endpoint
      response = join(client.raw().post("/rest/v3/counters/" + name + "/_get-and-set?value=10"));
      assertThat(response).hasReturnedText("90");
   }

   @Test
   public void testCounterNames() {
      String name = "v3-weak-one-%d";
      for (int i = 0; i < 5; i++) {
         createCounterV3(String.format(name, i), CounterConfiguration.builder(CounterType.WEAK).initialValue(5).build());
      }

      // Get counter names using v3 endpoint
      RestResponse response = join(client.raw().get("/rest/v3/counters", Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));
      assertThat(response).isOk();
      Json jsonNode = Json.read(response.body());
      Collection<String> counterNames = EmbeddedCounterManagerFactory.asCounterManager(cacheManagers.get(0)).getCounterNames();
      int size = jsonNode.asList().size();
      assertEquals(counterNames.size(), size);
      for (int i = 0; i < size; i++) {
         assertTrue(counterNames.contains(jsonNode.at(i).asString()));
      }
   }

   @Test
   public void testCounterCreation() {
      String counterName = "v3-counter-creation";
      createCounterV3(counterName, CounterConfiguration.builder(CounterType.WEAK).initialValue(1).build());
      assertThat(doCounterCreateRequestV3(counterName, CounterConfiguration.builder(CounterType.WEAK).initialValue(1).build())).isNotModified();
      assertThat(doCounterCreateRequestV3(counterName, CounterConfiguration.builder(CounterType.BOUNDED_STRONG).initialValue(2).build())).isNotModified();
   }

   private CompletionStage<RestResponse> doCounterCreateRequestV3(String name, CounterConfiguration configuration) {
      AbstractCounterConfiguration config = ConvertUtil.configToParsedConfig(name, configuration);
      RestEntity restEntity = RestEntity.create(APPLICATION_JSON, counterConfigToJson(config));
      return client.raw().post("/rest/v3/counters/" + name, restEntity);
   }

   private void createCounterV3(String name, CounterConfiguration configuration) {
      assertThat(doCounterCreateRequestV3(name, configuration)).isOk();
   }

   private void waitForCounterToReachV3(String name, int expectedValue) {
      eventually(() -> {
         RestResponse r = join(client.raw().get("/rest/v3/counters/" + name));
         assertThat(r).isOk();
         long value = Long.parseLong(r.body());
         return value == expectedValue;
      });
   }

}
