package org.infinispan.rest.framework;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.commons.util.concurrent.CompletionStages.join;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;
import static org.infinispan.rest.framework.Method.POST;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.Subject;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.framework.impl.ResourceManagerImpl;
import org.infinispan.rest.framework.impl.RestDispatcherImpl;
import org.infinispan.rest.framework.impl.SimpleRequest;
import org.infinispan.rest.framework.impl.SimpleRestResponse;
import org.infinispan.rest.operations.exceptions.ServiceUnavailableException;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.CustomAuditLoggerTest;
import org.infinispan.security.impl.Authorizer;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.test.TestingUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @since 10.0
 */
@Test(groups = "unit", testName = "rest.framework.RestDispatcherTest")
public class RestDispatcherTest {

   public void testDispatchUnavailable() {
      ResourceManagerImpl manager = new ResourceManagerImpl();
      manager.registerResource("ctx", new EchoResource());

      GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder().build();
      RestDispatcherImpl restDispatcher = new RestDispatcherImpl(manager, new Authorizer(globalConfiguration.security(), AuditContext.SERVER, "test", null));

      // Dispatcher is not initialized and this commands requires the CM start.
      RestRequest restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/context/var1/var2").build();
      CompletionStage<RestResponse> response = restDispatcher.dispatch(restRequest);
      final CompletionStage<RestResponse> finalResponse = response;
      assertThatThrownBy(() -> join(finalResponse)).hasCauseInstanceOf(ServiceUnavailableException.class);

      // This request can proceed without the CM start.
      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/context/echo").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals("echo", join(response).getEntity().toString());

      // After initializing the dispatcher, all commands should be accepted.
      restDispatcher.initialize();

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/context/echo").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals("echo", join(response).getEntity().toString());

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/context/var1/var2").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals("var1,var2", join(response).getEntity().toString());

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/context/var1/var2/var3?action=triple").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals("triple(var1,var2,var3)", join(response).getEntity().toString());
   }

   @Test
   public void testDispatch() {
      ResourceManagerImpl manager = new ResourceManagerImpl();
      manager.registerResource("/", new RootResource());
      manager.registerResource("ctx", new CounterResource());
      manager.registerResource("ctx", new MemoryResource());
      manager.registerResource("ctx", new EchoResource());
      manager.registerResource("ctx", new FileResource());

      GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder().build();

      RestDispatcherImpl restDispatcher = new RestDispatcherImpl(manager, new Authorizer(globalConfiguration.security(), AuditContext.SERVER, "test", null));
      restDispatcher.initialize();

      RestRequest restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/").build();
      CompletionStage<RestResponse> response = restDispatcher.dispatch(restRequest);
      assertEquals("Hello World!", join(response).getEntity().toString());

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/image.gif").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals("Hello World!", join(response).getEntity().toString());

      restRequest = new SimpleRequest.Builder().setMethod(POST).setPath("//ctx/counters/counter1").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals(200, join(response).getStatus());

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/counters/counter1?action=increment").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals(200, join(response).getStatus());

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/counters//counter1").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals("counter1->1", join(response).getEntity().toString());

      restRequest = new SimpleRequest.Builder().setMethod(POST).setPath("/ctx/jvm").build();
      assertNoResource(restDispatcher, restRequest);

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/jvm/memory").build();
      response = restDispatcher.dispatch(restRequest);
      assertTrue(Long.parseLong(join(response).getEntity().toString()) > 0);

      restRequest = new SimpleRequest.Builder().setMethod(HEAD).setPath("/ctx/jvm/memory").build();
      response = restDispatcher.dispatch(restRequest);
      assertTrue(Long.parseLong(join(response).getEntity().toString()) > 0);

      restRequest = new SimpleRequest.Builder().setMethod(HEAD).setPath("/ctx/v2/java-memory").build();
      response = restDispatcher.dispatch(restRequest);
      assertTrue(Long.parseLong(join(response).getEntity().toString()) > 0);

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/context/var1/var2").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals("var1,var2", join(response).getEntity().toString());

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/context/var1/var2/var3?action=triple").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals("triple(var1,var2,var3)", join(response).getEntity().toString());

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/context/var1/var2/var3?action=invalid").build();
      assertNoResource(restDispatcher, restRequest);

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/web/").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals("/ctx/web/index.html", join(response).getEntity().toString());

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/web/file.txt").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals("/ctx/web/file.txt", join(response).getEntity().toString());

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/web/dir/file.txt").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals("/ctx/web/dir/file.txt", join(response).getEntity().toString());

      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/web/dir1/dir2/file.txt").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals("/ctx/web/dir1/dir2/file.txt", join(response).getEntity().toString());

      // Create a resource named "{c}"
      restRequest = new SimpleRequest.Builder().setMethod(POST).setPath("/ctx/counters/{c}").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals(200, join(response).getStatus());

      // Read a resource named "{c}"
      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/counters/{c}").build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals("{c}->0", join(response).getEntity().toString());
   }

   @Test
   public void testDispatchWithAuthz() {
      final Subject ADMIN = TestingUtil.makeSubject("admin");
      final Subject LIFECYCLE = TestingUtil.makeSubject("lifecycle");

      ResourceManagerImpl manager = new ResourceManagerImpl();
      manager.registerResource("ctx", new SecureResource());

      CustomAuditLoggerTest.TestAuditLogger auditLogger = new CustomAuditLoggerTest.TestAuditLogger();
      GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder().security().authorization().enable().groupOnlyMapping(false)
            .auditLogger(auditLogger).principalRoleMapper(new IdentityRoleMapper()).build();


      RestDispatcherImpl restDispatcher = new RestDispatcherImpl(manager, new Authorizer(globalConfiguration.security(), AuditContext.SERVER, "test", null));
      restDispatcher.initialize();

      // Anonymous
      RestRequest restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/secure").build();
      CompletionStage<RestResponse> response = restDispatcher.dispatch(restRequest);
      Exceptions.expectCompletionException(SecurityException.class, response);
      assertEquals("Permission to ADMIN is DENY for user null", auditLogger.getLastRecord());

      // Wrong user
      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/secure").setSubject(LIFECYCLE).build();
      response = restDispatcher.dispatch(restRequest);
      Exceptions.expectCompletionException(SecurityException.class, response);
      assertEquals("Permission to ADMIN is DENY for user Subject:\n\tPrincipal: TestPrincipal [name=lifecycle]\n", auditLogger.getLastRecord());

      // Correct user
      restRequest = new SimpleRequest.Builder().setMethod(GET).setPath("/ctx/secure").setSubject(ADMIN).build();
      response = restDispatcher.dispatch(restRequest);
      assertEquals("Subject:\n\tPrincipal: TestPrincipal [name=admin]\n", join(response).getEntity().toString());
      assertEquals("Permission to ADMIN is ALLOW for user Subject:\n\tPrincipal: TestPrincipal [name=admin]\n", auditLogger.getLastRecord());
   }

   private void assertNoResource(RestDispatcher dispatcher, RestRequest restRequest) {
      try {
         CompletionStage<RestResponse> response = dispatcher.dispatch(restRequest);
         if (join(response) != null) Assert.fail();
      } catch (Exception ignored) {
      }
   }

   static class EchoResource implements ResourceHandler {

      @Override
      public Invocations getInvocations() {
         return new Invocations.Builder("test", "testing")
               .invocation().name("alwaysReady").method(GET).path("/context/{variable1}").requireCacheManagerStart(false).handleWith(this::singleVar)
               .invocation().name("doubleVars").method(GET).path("/context/{variable1}/{variable2}").handleWith(this::doubleVars)
               .invocation().name("tripleVars").method(GET).path("/context/{variable1}/{variable2}/{variable3}").withAction("triple").handleWith(this::tripleVarWithAction)
               .create();
      }

      private CompletionStage<RestResponse> tripleVarWithAction(RestRequest restRequest) {
         SimpleRestResponse.Builder responseBuilder = new SimpleRestResponse.Builder();
         String variable1 = restRequest.variables().get("variable1");
         String variable2 = restRequest.variables().get("variable2");
         String variable3 = restRequest.variables().get("variable3");
         String action = restRequest.getAction();
         return completedFuture(responseBuilder.entity(action + "(" + variable1 + "," + variable2 + "," + variable3 + ")").build());
      }

      private CompletionStage<RestResponse> doubleVars(RestRequest restRequest) {
         SimpleRestResponse.Builder responseBuilder = new SimpleRestResponse.Builder();
         String variable1 = restRequest.variables().get("variable1");
         String variable2 = restRequest.variables().get("variable2");
         return completedFuture(responseBuilder.entity(variable1 + "," + variable2).build());
      }

      private CompletionStage<RestResponse> singleVar(RestRequest restRequest) {
         SimpleRestResponse.Builder responseBuilder = new SimpleRestResponse.Builder();
         String variable = restRequest.variables().get("variable1");
         return completedFuture(responseBuilder.entity(variable).build());
      }
   }

   static class CounterResource implements ResourceHandler {

      private final Map<String, AtomicInteger> counters = new HashMap<>();

      @Override
      public Invocations getInvocations() {
         return new Invocations.Builder("test", "testing")
               .invocation().name("allCounters").method(GET).path("/counters").handleWith(this::listAllCounters)
               .invocation().name("addCounter").method(POST).path("/counters/{name}").handleWith(this::addCounter)
               .invocation().name("getCounter").method(GET).path("/counters/{name}").handleWith(this::getCounter)
               .invocation().name("incrementCounter").method(GET).path("/counters/{name}").withAction("increment").handleWith(this::incrementCounter)
               .create();
      }

      private CompletionStage<RestResponse> listAllCounters(RestRequest request) {
         SimpleRestResponse.Builder responseBuilder = new SimpleRestResponse.Builder();
         StringBuilder sb = new StringBuilder();
         counters.forEach((key, value) -> sb.append(key).append("->").append(value.get()));
         return completedFuture(responseBuilder.status(200).entity(sb.toString()).build());
      }

      private CompletionStage<RestResponse> addCounter(RestRequest request) {
         SimpleRestResponse.Builder responseBuilder = new SimpleRestResponse.Builder();
         String newCounterName = request.variables().get("name");
         if (newCounterName == null) {
            return completedFuture(responseBuilder.status(503).build());
         }
         counters.put(newCounterName, new AtomicInteger());
         return completedFuture(responseBuilder.status(200).build());
      }

      private CompletionStage<RestResponse> getCounter(RestRequest restRequest) {
         SimpleRestResponse.Builder responseBuilder = new SimpleRestResponse.Builder();
         String name = restRequest.variables().get("name");
         AtomicInteger atomicInteger = counters.get(name);
         if (atomicInteger == null) return completedFuture(responseBuilder.status(404).build());
         return completedFuture(responseBuilder.status(200).entity(name + "->" + atomicInteger.get()).build());
      }

      private CompletionStage<RestResponse> incrementCounter(RestRequest request) {
         SimpleRestResponse.Builder responseBuilder = new SimpleRestResponse.Builder();
         String name = request.variables().get("name");
         if (name == null) return completedFuture(responseBuilder.status(404).build());
         counters.get(name).incrementAndGet();
         return completedFuture(responseBuilder.status(200).build());
      }
   }

   static class MemoryResource implements ResourceHandler {

      @Override
      public Invocations getInvocations() {
         return new Invocations.Builder("test", "testing")
               .invocation().methods(GET, HEAD).path("/jvm/memory").path("/v2/java-memory").handleWith(this::showMemory)
               .create();
      }

      private CompletionStage<RestResponse> showMemory(RestRequest request) {
         return completedFuture(new SimpleRestResponse.Builder().entity(String.valueOf(Runtime.getRuntime().freeMemory())).build());
      }
   }

   static class RootResource implements ResourceHandler {

      @Override
      public Invocations getInvocations() {
         return new Invocations.Builder("test", "testing")
               .invocation().method(GET).path("/").path("/image.gif").handleWith(this::serveStaticResource)
               .create();
      }

      private CompletionStage<RestResponse> serveStaticResource(RestRequest restRequest) {
         return completedFuture(new SimpleRestResponse.Builder().entity("Hello World!").build());
      }
   }

   static class FileResource implements ResourceHandler {

      @Override
      public Invocations getInvocations() {
         return new Invocations.Builder("test", "testing")
               .invocation().method(GET).path("/web").path("/web/*").handleWith(this::handleGet)
               .create();
      }

      private CompletionStage<RestResponse> handleGet(RestRequest restRequest) {
         String path = restRequest.path();
         if (path.endsWith("web/")) {
            path = path.concat("index.html");
         }
         return completedFuture(new SimpleRestResponse.Builder().entity(path).build());
      }

   }

   static class SecureResource implements ResourceHandler {

      @Override
      public Invocations getInvocations() {
         return new Invocations.Builder("test", "testing")
               .invocation().method(GET).path("/secure").permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER).handleWith(this::handleGet)
               .create();
      }

      private CompletionStage<RestResponse> handleGet(RestRequest restRequest) {
         return completedFuture(new SimpleRestResponse.Builder().entity(restRequest.getSubject().toString()).build());
      }
   }
}
