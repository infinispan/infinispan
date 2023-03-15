package org.infinispan.rest.resources;

import static org.infinispan.client.rest.configuration.Protocol.HTTP_11;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_20;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.rest.RestCounterClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestSchemaClient;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.ConvertUtil;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Test for calling multiple resources concurrently.
 *
 * @since 12.0
 */
@Test(groups = "functional", testName = "rest.MultiResourceTest")
public class MultiResourceTest extends AbstractRestResourceTest {

   private ExecutorService service;

   @Override
   public Object[] factory() {
      return new Object[]{
            new MultiResourceTest().withSecurity(false).protocol(HTTP_11).ssl(false).browser(false),
            new MultiResourceTest().withSecurity(false).protocol(HTTP_11).ssl(false).browser(true),
            new MultiResourceTest().withSecurity(true).protocol(HTTP_20).ssl(false).browser(false),
            new MultiResourceTest().withSecurity(true).protocol(HTTP_20).ssl(false).browser(true),
            new MultiResourceTest().withSecurity(true).protocol(HTTP_11).ssl(true).browser(false),
            new MultiResourceTest().withSecurity(true).protocol(HTTP_11).ssl(true).browser(true),
            new MultiResourceTest().withSecurity(true).protocol(HTTP_20).ssl(true).browser(false),
            new MultiResourceTest().withSecurity(true).protocol(HTTP_20).ssl(true).browser(true),
      };
   }

   @BeforeMethod
   public void setUp() throws Exception {
      service = Executors.newFixedThreadPool(5);
      createCaches("cache1", "cache2");
      createCounters("counter1", "counter2");
      createSchema("1.proto", "message A1 {}");
      createSchema("2.proto", "message B1 {}");
   }

   @AfterMethod
   public void tearDown() {
      join(client.cache("cache1").delete());
      join(client.cache("cache2").delete());
      join(client.counter("counter1").delete());
      join(client.counter("counter2").delete());
      join(client.schemas().delete("1.proto"));
      join(client.schemas().delete("2.proto"));
      service.shutdown();
   }


   @Test
   public void testMultiThreadedOps() throws Exception {
      CountDownLatch startLatch = new CountDownLatch(1);

      CompletableFuture<Boolean> r1 = doCacheReadWrite(startLatch, "cache1");
      CompletableFuture<Boolean> r2 = doCacheReadWrite(startLatch, "cache2");
      CompletableFuture<Boolean> r3 = doCounterReadAndWrite(startLatch, "counter1");
      CompletableFuture<Boolean> r4 = doCounterReadAndWrite(startLatch, "counter2");
      CompletableFuture<Boolean> r5 = doSchemaReadWrite(startLatch, "1.proto", "A");
      CompletableFuture<Boolean> r6 = doSchemaReadWrite(startLatch, "2.proto", "B");

      List<CompletableFuture<Boolean>> futures = Arrays.asList(r1, r2, r3, r4, r5, r6);

      startLatch.countDown();

      for (CompletableFuture<Boolean> future : futures) {
         CompletableFutures.await(future, 10, TimeUnit.SECONDS);
         assertTrue(future.get());
      }
   }

   private CompletableFuture<Boolean> doSchemaReadWrite(CountDownLatch startLatch, String protoName, String messagePrefix) {
      return CompletableFuture.supplyAsync(() -> {
         try {
            String messageFormat = "message %s%d {}";
            startLatch.await();
            createSchema(protoName, String.format(messageFormat, messagePrefix, 1));
            createSchema(protoName, String.format(messageFormat, messagePrefix, 2));
            createSchema(protoName, String.format(messageFormat, messagePrefix, 3));
            String lastSchema = String.format(messageFormat, messagePrefix, 4);
            createSchema(protoName, lastSchema);
            assertEquals(lastSchema, getProtobuf(protoName));
            return true;
         } catch (Throwable e) {
            e.printStackTrace();
            return false;
         }
      }, service);
   }

   private CompletableFuture<Boolean> doCounterReadAndWrite(CountDownLatch startLatch, String counterName) {
      return CompletableFuture.supplyAsync(() -> {
         try {
            startLatch.await();
            callCounterOp(counterName, "increment");
            callCounterOp(counterName, "increment");
            callCounterOp(counterName, "increment");
            callCounterOp(counterName, "increment");
            callCounterOp(counterName, "increment");
            callCounterOp(counterName, "decrement");
            callCounterOp(counterName, "decrement");
            callCounterOp(counterName, "decrement");
            RestCounterClient counterClient = client.counter(counterName);
            eventually(() -> {
               RestResponse r = join(counterClient.get());
               ResponseAssertion.assertThat(r).isOk();
               long value = Long.parseLong(r.getBody());
               return value == 2;
            });
            return true;
         } catch (Throwable e) {
            e.printStackTrace();
            return false;
         }
      }, service);
   }

   private CompletableFuture<Boolean> doCacheReadWrite(CountDownLatch startLatch, String cacheName) {
      return CompletableFuture.supplyAsync(() -> {
         try {
            startLatch.await();
            changeValue(cacheName, "1", "1");
            changeValue(cacheName, "2", "2");
            changeValue(cacheName, "3", "3");
            changeValue(cacheName, "1", "1'");
            changeValue(cacheName, "2", "2'");
            changeValue(cacheName, "3", "3'");
            assertEquals("1'", getValue(cacheName, "1"));
            assertEquals("2'", getValue(cacheName, "2"));
            assertEquals("3'", getValue(cacheName, "3"));
            return true;
         } catch (Throwable e) {
            e.printStackTrace();
            return false;
         }
      }, service);
   }

   private void callCounterOp(String name, String op) {
      RestCounterClient counterClient = client.counter(name);
      RestResponse response = null;
      switch (op) {
         case "increment":
            response = join(counterClient.increment());
            break;
         case "decrement":
            response = join(counterClient.decrement());
            break;
         default:
            Assert.fail("Invalid operation " + op);
      }
      ResponseAssertion.assertThat(response).isOk();
   }

   private String getValue(String cacheName, String key) {
      RestResponse response = join(client.cache(cacheName).get(key));
      ResponseAssertion.assertThat(response).isOk();
      return response.getBody();
   }

   private void changeValue(String cacheName, String key, String value) {
      RestResponse response = join(client.cache(cacheName).put(key, value));
      ResponseAssertion.assertThat(response).isOk();
   }

   private void createSchema(String name, String value) throws Exception {
      RestSchemaClient schemas = client.schemas();
      RestResponse response = join(schemas.put(name, value));
      ResponseAssertion.assertThat(response).isOk();
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode jsonNode = objectMapper.readTree(response.getBody());
      assertEquals("null", jsonNode.get("error").asText());
   }

   private String getProtobuf(String name) {
      RestSchemaClient schemas = client.schemas();
      RestResponse response = join(schemas.get(name));
      ResponseAssertion.assertThat(response).isOk();
      return response.getBody();
   }

   private void createCounters(String... names) {
      CounterConfiguration configuration = CounterConfiguration
            .builder(CounterType.BOUNDED_STRONG)
            .lowerBound(0).upperBound(100)
            .initialValue(0).build();
      for (String counterName : names) {
         AbstractCounterConfiguration config = ConvertUtil.configToParsedConfig(counterName, configuration);
         RestResponse response = join(client.counter(counterName).create(RestEntity.create(APPLICATION_JSON, counterConfigToJson(config))));
         ResponseAssertion.assertThat(response).isOk();
      }
   }

   private void createCaches(String... names) {
      RestEntity jsonEntity = RestEntity.create(APPLICATION_JSON, "{}");

      for (String cacheName : names) {
         CompletionStage<RestResponse> response = client.cache(cacheName).createWithConfiguration(jsonEntity, CacheContainerAdmin.AdminFlag.VOLATILE);
         ResponseAssertion.assertThat(response).isOk();
      }
   }
}
