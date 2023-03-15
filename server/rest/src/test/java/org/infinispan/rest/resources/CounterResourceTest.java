package org.infinispan.rest.resources;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestCounterClient;
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

@Test(groups = "functional", testName = "rest.CounterResourceTest")
public class CounterResourceTest extends AbstractRestResourceTest {

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
      CounterManager counterManager = EmbeddedCounterManagerFactory.asCounterManager(cm);
      counterManager.defineCounter("weak", CounterConfiguration.builder(CounterType.WEAK).build());
      counterManager.defineCounter("strong", CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).build());
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new CounterResourceTest().withSecurity(false).browser(false),
            new CounterResourceTest().withSecurity(false).browser(true),
            new CounterResourceTest().withSecurity(true).browser(false),
            new CounterResourceTest().withSecurity(true).browser(true),
      };
   }

   @Test
   public void testWeakCounterLifecycle() {
      CounterConfiguration counterConfig = CounterConfiguration.builder(CounterType.WEAK)
            .initialValue(5).storage(Storage.VOLATILE).concurrencyLevel(6).build();
      createCounter("sample-counter", counterConfig);

      RestCounterClient counterClient = client.counter("sample-counter");

      RestResponse response = join(counterClient.configuration(APPLICATION_JSON_TYPE));
      Json jsonNode = Json.read(response.getBody());
      Json config = jsonNode.at("weak-counter");
      assertEquals(config.at("initial-value").asInteger(), 5);
      assertEquals(config.at("storage").asString(), "VOLATILE");
      assertEquals(config.at("concurrency-level").asInteger(), 6);

      response = join(counterClient.delete());
      assertThat(response).isOk();

      response = join(counterClient.configuration());
      assertThat(response).isNotFound();
   }

   @Test
   public void testWeakCounterOps() {
      String name = "weak-test";
      createCounter(name, CounterConfiguration.builder(CounterType.WEAK).initialValue(5).build());

      RestCounterClient counterClient = client.counter(name);

      CompletionStage<RestResponse> response = counterClient.increment();
      assertThat(response).hasNoContent();
      waitForCounterToReach(name, 6);

      response = counterClient.increment();
      assertThat(response).hasNoContent();
      waitForCounterToReach(name, 7);

      response = counterClient.decrement();
      assertThat(response).hasNoContent();
      waitForCounterToReach(name, 6);

      response = counterClient.decrement();
      assertThat(response).hasNoContent();
      waitForCounterToReach(name, 5);

      response = counterClient.add(10);
      assertThat(response).hasNoContent();
      waitForCounterToReach(name, 15);

      response = counterClient.reset();
      assertThat(response).hasNoContent();
      waitForCounterToReach(name, 5);
   }

   @Test
   public void testStrongCounterOps() {
      String name = "strong-test";
      createCounter(name, CounterConfiguration.builder(CounterType.BOUNDED_STRONG).lowerBound(0).upperBound(100)
            .initialValue(0).build());

      RestCounterClient counterClient = client.counter(name);

      CompletionStage<RestResponse> response = counterClient.increment();
      assertThat(response).hasReturnedText("1");

      response = counterClient.increment();
      assertThat(response).hasReturnedText("2");

      response = counterClient.decrement();
      assertThat(response).hasReturnedText("1");

      response = counterClient.decrement();
      assertThat(response).hasReturnedText("0");

      response = counterClient.add(35);
      assertThat(response).hasReturnedText("35");
      waitForCounterToReach(name, 35);

      response = counterClient.compareAndSet(5, 32);
      assertThat(response).hasReturnedText("false");

      response = counterClient.compareAndSet(35, 50);
      assertThat(response).hasReturnedText("true");
      waitForCounterToReach(name, 50);

      response = counterClient.compareAndSwap(50, 90);
      assertThat(response).hasReturnedText("50");

      response = counterClient.get();
      assertThat(response).hasReturnedText("90");

      response = counterClient.getAndSet(10);
      assertThat(response).hasReturnedText("90");
   }

   @Test
   public void testCounterNames() {
      String name = "weak-one-%d";
      for (int i = 0; i < 5; i++) {
         createCounter(String.format(name, i), CounterConfiguration.builder(CounterType.WEAK).initialValue(5).build());
      }

      RestResponse response = join(client.counters());
      assertThat(response).isOk();
      Json jsonNode = Json.read(response.getBody());
      Collection<String> counterNames = EmbeddedCounterManagerFactory.asCounterManager(cacheManagers.get(0)).getCounterNames();
      int size = jsonNode.asList().size();
      assertEquals(counterNames.size(), size);
      for (int i = 0; i < size; i++) {
         assertTrue(counterNames.contains(jsonNode.at(i).asString()));
      }
   }

   @Test
   public void testCounterCreation() {
      String counterName = "counter-creation";
      createCounter(counterName, CounterConfiguration.builder(CounterType.WEAK).initialValue(1).build());
      assertThat(doCounterCreateRequest(counterName, CounterConfiguration.builder(CounterType.WEAK).initialValue(1).build())).isNotModified();
      assertThat(doCounterCreateRequest(counterName, CounterConfiguration.builder(CounterType.BOUNDED_STRONG).initialValue(2).build())).isNotModified();
   }

   private CompletionStage<RestResponse> doCounterCreateRequest(String name, CounterConfiguration configuration) {
      AbstractCounterConfiguration config = ConvertUtil.configToParsedConfig(name, configuration);
      RestEntity restEntity = RestEntity.create(APPLICATION_JSON, counterConfigToJson(config));
      return client.counter(name).create(restEntity);
   }

   private void createCounter(String name, CounterConfiguration configuration) {
      assertThat(doCounterCreateRequest(name, configuration)).isOk();
   }

   private void waitForCounterToReach(String name, int i) {
      RestCounterClient counterClient = client.counter(name);
      eventually(() -> {
         RestResponse r = join(counterClient.get());
         assertThat(r).isOk();
         long value = Long.parseLong(r.getBody());
         return value == i;
      });
   }

}
