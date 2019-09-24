package org.infinispan.rest.resources;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.http.HttpHeaders;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.ConvertUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Test(groups = "functional", testName = "rest.CounterResourceTest")
public class CounterResourceTest extends AbstractRestResourceTest {

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
      CounterManager counterManager = EmbeddedCounterManagerFactory.asCounterManager(cm);
      counterManager.defineCounter("weak", CounterConfiguration.builder(CounterType.WEAK).build());
      counterManager.defineCounter("strong", CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).build());
   }

   @Test
   public void testWeakCounterLifecycle() throws Exception {
      CounterConfiguration counterConfig = CounterConfiguration.builder(CounterType.WEAK)
            .initialValue(5).storage(Storage.VOLATILE).concurrencyLevel(6).build();
      createCounter("sample-counter", counterConfig);

      String url = String.format("http://localhost:%d/rest/v2/counters/sample-counter", restServer().getPort());

      ContentResponse response = client.newRequest(url + "/config").accept(APPLICATION_JSON_TYPE).send();
      JsonNode jsonNode = new ObjectMapper().readTree(response.getContentAsString());
      JsonNode config = jsonNode.get("weak-counter");
      assertEquals(config.get("initial-value").asInt(), 5);
      assertEquals(config.get("storage").asText(), "VOLATILE");
      assertEquals(config.get("concurrency-level").asInt(), 6);

      response = client.newRequest(url).method(HttpMethod.DELETE).send();
      ResponseAssertion.assertThat(response).isOk();

      response = client.newRequest(url + "/config").accept(APPLICATION_JSON_TYPE).send();
      ResponseAssertion.assertThat(response).isNotFound();
   }

   @Test
   public void testWeakCounterOps() throws Exception {
      String name = "weak-test";
      createCounter(name, CounterConfiguration.builder(CounterType.WEAK).initialValue(5).build());

      ContentResponse response = callCounterOp(name, "increment");
      assertThat(response).hasNoContent();
      waitForCounterToReach(name, 6);

      response = callCounterOp(name, "increment");
      assertThat(response).hasNoContent();
      waitForCounterToReach(name, 7);

      response = callCounterOp(name, "decrement");
      assertThat(response).hasNoContent();
      waitForCounterToReach(name, 6);

      response = callCounterOp(name, "decrement");
      assertThat(response).hasNoContent();
      waitForCounterToReach(name, 5);

      response = callCounterOp(name, "add", "delta=10");
      assertThat(response).hasNoContent();
      waitForCounterToReach(name, 15);

      response = callCounterOp(name, "reset");
      assertThat(response).hasNoContent();
      waitForCounterToReach(name, 5);
   }

   @Test
   public void testStrongCounterOps() throws Exception {
      String name = "strong-test";
      createCounter(name, CounterConfiguration.builder(CounterType.BOUNDED_STRONG).lowerBound(0).upperBound(100)
            .initialValue(0).build());

      ContentResponse response = callCounterOp(name, "increment");
      assertThat(response).hasReturnedText("1");

      response = callCounterOp(name, "increment");
      assertThat(response).hasReturnedText("2");

      response = callCounterOp(name, "decrement");
      assertThat(response).hasReturnedText("1");

      response = callCounterOp(name, "decrement");
      assertThat(response).hasReturnedText("0");

      response = callCounterOp(name, "add", "delta=35");
      assertThat(response).hasReturnedText("35");
      waitForCounterToReach(name, 35);

      response = callCounterOp(name, "compareAndSet", "expect=5", "update=12");
      assertThat(response).hasReturnedText("false");

      response = callCounterOp(name, "compareAndSet", "expect=35", "update=50");
      assertThat(response).hasReturnedText("true");
      waitForCounterToReach(name, 50);

      response = callCounterOp(name, "compareAndSwap", "expect=50", "update=90");
      assertThat(response).hasReturnedText("50");

      response = getCounterValue(name);
      assertThat(response).hasReturnedText("90");
   }

   @Test
   public void testCounterNames() throws Exception {
      ObjectMapper objectMapper = new ObjectMapper();
      String name = "weak-one-%d";
      for (int i = 0; i < 5; i++) {
         createCounter(String.format(name, i), CounterConfiguration.builder(CounterType.WEAK).initialValue(5).build());
      }

      String url = String.format("http://localhost:%d/rest/v2/counters", restServer().getPort());
      ContentResponse response = client.newRequest(url).send();
      ResponseAssertion.assertThat(response).isOk();
      JsonNode jsonNode = objectMapper.readTree(response.getContent());
      Collection<String> counterNames = EmbeddedCounterManagerFactory.asCounterManager(cacheManagers.get(0)).getCounterNames();
      assertEquals(counterNames.size(), jsonNode.size());
      for (int i = 0; i < jsonNode.size(); i++) {
         assertTrue(counterNames.contains(jsonNode.get(i).asText()));
      }
   }

   private void createCounter(String name, CounterConfiguration configuration) throws InterruptedException, ExecutionException, TimeoutException {
      String url = String.format("http://localhost:%d/rest/v2/counters/" + name, restServer().getPort());
      AbstractCounterConfiguration config = ConvertUtil.configToParsedConfig(name, configuration);
      ContentResponse response = client
            .newRequest(url)
            .method(HttpMethod.POST)
            .content(new StringContentProvider(new JsonWriter().toJSON(config)))
            .header(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON_TYPE)
            .send();
      ResponseAssertion.assertThat(response).isOk();
   }

   private void waitForCounterToReach(String name, int i) {
      String url = String.format("http://localhost:%d/rest/v2/counters/" + name, restServer().getPort());
      eventually(() -> {
         ContentResponse r = client.newRequest(url).send();
         ResponseAssertion.assertThat(r).isOk();
         long value = Long.parseLong(r.getContentAsString());
         return value == i;
      });
   }

   private ContentResponse callCounterOp(String name, String op, String... params) throws Exception {
      String urlParams = params.length == 0 ? "" : "&" + String.join("&", params);
      String url = String.format("http://localhost:%d/rest/v2/counters/%s?action=%s&%s", restServer().getPort(),
            name, op, urlParams);
      ContentResponse response = client.newRequest(url).send();
      ResponseAssertion.assertThat(response).isOk();
      return response;
   }

   private ContentResponse getCounterValue(String name) throws Exception {
      String url = String.format("http://localhost:%d/rest/v2/counters/%s", restServer().getPort(), name);
      ContentResponse response = client.newRequest(url).send();
      ResponseAssertion.assertThat(response).isOk();
      return response;
   }

}
