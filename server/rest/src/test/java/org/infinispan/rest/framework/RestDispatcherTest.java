package org.infinispan.rest.framework;

import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;
import static org.infinispan.rest.framework.Method.POST;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.framework.impl.ResourceManagerImpl;
import org.infinispan.rest.framework.impl.RestDispatcherImpl;
import org.infinispan.rest.framework.impl.SimpleRequest;
import org.infinispan.rest.framework.impl.SimpleRestResponse;
import org.infinispan.rest.operations.exceptions.ResourceNotFoundException;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @since 10.0
 */
@Test(groups = "unit", testName = "rest.framework.RestDispatcherTest")
public class RestDispatcherTest {

   @Test
   public void testDispatch() throws Exception {
      ResourceManagerImpl manager = new ResourceManagerImpl("ctx");
      manager.registerResource(new RootResource());
      manager.registerResource(new CounterResource());
      manager.registerResource(new MemoryResource());
      manager.registerResource(new EchoResource());

      RestDispatcherImpl restDispatcher = new RestDispatcherImpl(manager);

      RestRequest restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/").build();
      RestResponse response = restDispatcher.dispatch(restRequest);
      assertEquals("Hello World!", response.getEntity().toString());

      restRequest = new SimpleRequest.Builder().setMethod(POST).setPath("/ctx/counters/counter1").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals(200, response.getStatus());

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/counters/counter1?action=increment").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals(200, response.getStatus());

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/counters/counter1").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals("counter1->1", response.getEntity().toString());

      restRequest = new SimpleRequest.Builder().setMethod(POST).setPath("/ctx/jvm").build();
      assertNoResource(restDispatcher, restRequest);

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/jvm/memory").build();
      response = restDispatcher.dispatch(restRequest);
      assertTrue(Long.valueOf(response.getEntity().toString()) > 0);

      restRequest = new SimpleRequest.Builder().setMethod(HEAD).setPath("/ctx/jvm/memory").build();
      response = restDispatcher.dispatch(restRequest);
      assertTrue(Long.valueOf(response.getEntity().toString()) > 0);

      restRequest = new SimpleRequest.Builder().setMethod(HEAD).setPath("/ctx/v2/java-memory").build();
      response = restDispatcher.dispatch(restRequest);
      assertTrue(Long.valueOf(response.getEntity().toString()) > 0);

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/context/var1/var2").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals("var1,var2", response.getEntity().toString());

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/context/var1/var2/var3?action=triple").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals("triple(var1,var2,var3)", response.getEntity().toString());

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/context/var1/var2/var3?action=invalid").build();
      assertNoResource(restDispatcher, restRequest);
   }

   private void assertNoResource(RestDispatcher dispatcher, RestRequest restRequest) {
      try {
         RestResponse response = dispatcher.dispatch(restRequest);
         if (response != null) Assert.fail();
      } catch (ResourceNotFoundException ignored) {
      }
   }

   class EchoResource implements ResourceHandler {

      @Override
      public Invocations getInvocations() {
         return new Invocations.Builder()
               .invocation().name("doubleVars").method(GET).path("/context/{variable1}/{variable2}").handleWith(this::doubleVars)
               .invocation().name("tripleVars").method(GET).path("/context/{variable1}/{variable2}/{variable3}").withAction("triple").handleWith(this::tripleVarWithAction)
               .create();
      }

      private RestResponse tripleVarWithAction(RestRequest restRequest) {
         SimpleRestResponse.Builder responseBuilder = new SimpleRestResponse.Builder();
         String variable1 = restRequest.variables().get("variable1");
         String variable2 = restRequest.variables().get("variable2");
         String variable3 = restRequest.variables().get("variable3");
         String action = restRequest.getAction();
         return responseBuilder.entity(action + "(" + variable1 + "," + variable2 + "," + variable3 + ")").build();
      }

      private RestResponse doubleVars(RestRequest restRequest) {
         SimpleRestResponse.Builder responseBuilder = new SimpleRestResponse.Builder();
         String variable1 = restRequest.variables().get("variable1");
         String variable2 = restRequest.variables().get("variable2");
         return responseBuilder.entity(variable1 + "," + variable2).build();
      }
   }

   class CounterResource implements ResourceHandler {

      private final Map<String, AtomicInteger> counters = new HashMap<>();

      @Override
      public Invocations getInvocations() {
         return new Invocations.Builder()
               .invocation().name("allCounters").method(GET).path("/counters").handleWith(this::listAllCounters)
               .invocation().name("addCounter").method(POST).path("/counters/{name}").handleWith(this::addCounter)
               .invocation().name("getCounter").method(GET).path("/counters/{name}").handleWith(this::getCounter)
               .invocation().name("incrementCounter").method(GET).path("/counters/{name}").withAction("increment").handleWith(this::incrementCounter)
               .create();
      }

      private RestResponse listAllCounters(RestRequest request) {
         SimpleRestResponse.Builder responseBuilder = new SimpleRestResponse.Builder();
         StringBuilder sb = new StringBuilder();
         counters.forEach((key, value) -> sb.append(key).append("->").append(value.get()));
         return responseBuilder.status(200).entity(sb.toString()).build();
      }

      private RestResponse addCounter(RestRequest request) {
         SimpleRestResponse.Builder responseBuilder = new SimpleRestResponse.Builder();
         String newCounterName = request.variables().get("name");
         if (newCounterName == null) {
            return responseBuilder.status(503).build();
         }
         counters.put(newCounterName, new AtomicInteger());
         return responseBuilder.status(200).build();
      }

      private RestResponse getCounter(RestRequest restRequest) {
         SimpleRestResponse.Builder responseBuilder = new SimpleRestResponse.Builder();
         String name = restRequest.variables().get("name");
         AtomicInteger atomicInteger = counters.get(name);
         if (atomicInteger == null) return responseBuilder.status(404).build();
         return responseBuilder.status(200).entity(name + "->" + atomicInteger.get()).build();
      }

      private RestResponse incrementCounter(RestRequest request) {
         SimpleRestResponse.Builder responseBuilder = new SimpleRestResponse.Builder();
         String name = request.variables().get("name");
         if (name == null) return responseBuilder.status(404).build();
         counters.get(name).incrementAndGet();
         return responseBuilder.status(200).build();
      }

   }

   class MemoryResource implements ResourceHandler {

      @Override
      public Invocations getInvocations() {
         return new Invocations.Builder()
               .invocation().methods(GET, HEAD).path("/jvm/memory").path("/v2/java-memory").handleWith(this::showMemory)
               .create();
      }

      private RestResponse showMemory(RestRequest request) {
         return new SimpleRestResponse.Builder().entity(String.valueOf(Runtime.getRuntime().freeMemory())).build();
      }
   }

   class RootResource implements ResourceHandler {

      @Override
      public Invocations getInvocations() {
         return new Invocations.Builder()
               .invocation().method(GET).path("/").handleWith(this::serveStaticResource)
               .create();
      }

      private RestResponse serveStaticResource(RestRequest restRequest) {
         return new SimpleRestResponse.Builder().entity("Hello World!").build();
      }

   }
}
