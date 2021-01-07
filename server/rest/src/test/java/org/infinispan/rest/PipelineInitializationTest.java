package org.infinispan.rest;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test Netty pipeline initialization with multiple simultaneous clients.
 */
@Test(groups = "functional", testName = "rest.CorsInitializationTest")
public class PipelineInitializationTest extends AbstractInfinispanTest {

   private RestServerHelper restServer;
   private RestClient client1, client2;

   @BeforeMethod(alwaysRun = true)
   public void beforeMethod() {
      restServer = RestServerHelper.defaultRestServer().start(TestResourceTracker.getCurrentTestShortName());
      RestClientConfigurationBuilder configurationBuilder = new RestClientConfigurationBuilder();
      configurationBuilder.addServer().host(restServer.getHost()).port(restServer.getPort());
      client1 = RestClient.forConfiguration(configurationBuilder.build());
      client2 = RestClient.forConfiguration(configurationBuilder.build());
   }

   @AfterMethod(alwaysRun = true)
   public void afterMethod() throws IOException {
      restServer.clear();
      if (restServer != null) {
         restServer.stop();
         client1.close();
         client2.close();
      }
   }

   private Supplier<Integer> createTask(RestClient client, CountDownLatch latch) {
      return () -> {
         try {
            latch.await();
         } catch (InterruptedException ignored) {
         }
         RestResponse response = await(client.caches());
         return response.getStatus();
      };
   }

   @Test
   public void testInitializationRules() throws InterruptedException, ExecutionException {
      int numTasks = 5;

      ExecutorService executorService = Executors.newFixedThreadPool(numTasks);
      CountDownLatch startLatch = new CountDownLatch(1);

      List<Supplier<Integer>> suppliers = new ArrayList<>();
      for (int i = 0; i < numTasks; i++) {
         RestClient client = i % 2 == 0 ? client1 : client2;
         suppliers.add(createTask(client, startLatch));
      }

      List<CompletableFuture<Integer>> results = suppliers.stream().map(s -> supplyAsync(s, executorService))
            .collect(Collectors.toList());

      startLatch.countDown();

      executorService.shutdown();
      assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));

      for (CompletableFuture<Integer> result : results) {
         assertFalse(result.isCompletedExceptionally());
         assertEquals((int) result.get(), 200);
      }
   }

}
