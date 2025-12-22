package org.infinispan.rest.framework.impl;

import static org.infinispan.rest.framework.LookupResult.Status.FOUND;
import static org.infinispan.rest.framework.LookupResult.Status.INVALID_ACTION;
import static org.infinispan.rest.framework.LookupResult.Status.INVALID_METHOD;
import static org.infinispan.rest.framework.LookupResult.Status.NOT_FOUND;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;
import static org.infinispan.rest.framework.Method.POST;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.Arrays;

import org.infinispan.rest.framework.LookupResult;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.RegistrationException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "rest.framework.ResourceManagerImplTest")
public class ResourceManagerImplTest {

   private ResourceManagerImpl resourceManager;

   @BeforeMethod
   public void setUp() {
      resourceManager = new ResourceManagerImpl();
   }

   @Test(expectedExceptions = RegistrationException.class, expectedExceptionsMessageRegExp = "ISPN.*Cannot register invocation 'HandlerB'.*")
   public void testShouldPreventOverlappingVariablePaths() {
      registerHandler("/", "HandlerA", "/{pathA}");
      registerHandler("/", "HandlerB", "/{pathB}");
   }

   @Test(expectedExceptions = RegistrationException.class, expectedExceptionsMessageRegExp = "ISPN.*/path/\\{b\\}.*conflicts with.*/path/a.*")
   public void testPreventAmbiguousPaths() {
      registerHandler("/", "handler1", "/path/a");
      registerHandler("/", "handler2", "/path/{b}");
   }

   @Test(expectedExceptions = RegistrationException.class, expectedExceptionsMessageRegExp = "ISPN.*/path/\\{b\\}.*conflicts with.*/path/\\{a\\}.*")
   public void testPreventVariableNameConflict() {
      registerHandler("/", "handler1", "/path/{a}/");
      registerHandler("/", "handler1", "/path/{b}/sub-path");
   }

   public void testAllowRegistrationForDifferentMethods() {
      registerHandler("/", "HandlerA", GET, "/root/a/{pathA}");
      registerHandler("/", "HandlerB", POST, "/root/a/{pathA}");
   }

   public void testLookupRepeatedPaths() {
      registerHandler("/", "Handler", GET, "/root/path/path");
      assertNotNull(resourceManager.lookupResource(GET, "/root/path/path"));
   }

   @Test
   public void testLookupHandler() {
      registerHandler("root", "CompaniesHandler", "/companies", "/companies/{company}", "/companies/{company}/{id}");
      registerHandler("root", "StocksHandler", "/stocks/{stock}", "/stocks/{stock}/{currency}");
      registerHandler("root", "DirectorsHandler", "/directors", "/directors/director", "/directors/director/{personId}");
      registerHandler("root", "InfoHandler", "/info", "/info/jvm", "/info/jvm/{format}", "/info/{format}/{encoding}");

      assertEquals(NOT_FOUND, resourceManager.lookupResource(GET, "/root/dummy").getStatus());
      assertEquals(NOT_FOUND, resourceManager.lookupResource(GET, "/fake/").getStatus());
      assertEquals(NOT_FOUND, resourceManager.lookupResource(GET, "/").getStatus());

      assertEquals(NOT_FOUND, resourceManager.lookupResource(GET, "/root/stocks").getStatus());
      assertEquals(NOT_FOUND, resourceManager.lookupResource(GET, "/root/stocks/2/USD/1").getStatus());
      assertInvocation(resourceManager.lookupResource(GET, "/root/stocks/2"), "StocksHandler");
      assertInvocation(resourceManager.lookupResource(GET, "/root/stocks/2/USD"), "StocksHandler");

      assertInvocation(resourceManager.lookupResource(GET, "/root/directors"), "DirectorsHandler");
      assertInvocation(resourceManager.lookupResource(GET, "/root/directors/director"), "DirectorsHandler");
      assertInvocation(resourceManager.lookupResource(GET, "/root/directors/director/John"), "DirectorsHandler");
      assertEquals(NOT_FOUND, resourceManager.lookupResource(GET, "/root/directors/1345").getStatus());
      assertEquals(NOT_FOUND, resourceManager.lookupResource(GET, "/root/directors/director/Tim/123").getStatus());

      assertInvocation(resourceManager.lookupResource(GET, "/root/companies"), "CompaniesHandler");

      assertInvocation(resourceManager.lookupResource(GET, "/root/info"), "InfoHandler");
      assertInvocation(resourceManager.lookupResource(GET, "/root/info/jvm"), "InfoHandler");
      assertInvocation(resourceManager.lookupResource(GET, "/root/info/jvm/json"), "InfoHandler");
      assertInvocation(resourceManager.lookupResource(GET, "/root/info/json/zip"), "InfoHandler");
   }

   @Test
   public void testLookupStatuses() {
      registerHandler("ctx", "handler1", "/items/{item}");
      registerHandlerWithAction("ctx", new Method[]{GET}, "handler2", "clear", "/items/{item}/{sub}");

      LookupResult lookupResult = resourceManager.lookupResource(GET, "/invalid");
      assertEquals(NOT_FOUND, lookupResult.getStatus());

      lookupResult = resourceManager.lookupResource(GET, "/ctx/items/1");
      assertEquals(FOUND, lookupResult.getStatus());

      lookupResult = resourceManager.lookupResource(DELETE, "/ctx/items/1");
      assertEquals(INVALID_METHOD, lookupResult.getStatus());

      lookupResult = resourceManager.lookupResource(GET, "/ctx/items/1/1", "clear");
      assertEquals(FOUND, lookupResult.getStatus());

      lookupResult = resourceManager.lookupResource(GET, "/ctx/items/1/1");
      assertEquals(INVALID_ACTION, lookupResult.getStatus());

      lookupResult = resourceManager.lookupResource(GET, "/ctx/items/1/1", "invalid");
      assertEquals(INVALID_ACTION, lookupResult.getStatus());

      lookupResult = resourceManager.lookupResource(GET, "/ctx/items/1", "invalid");
      assertEquals(INVALID_ACTION, lookupResult.getStatus());
   }

   private void assertInvocation(LookupResult result, String name) {
      assertEquals(name, result.getInvocation().name());
   }

   private void registerHandler(String ctx, String handlerName, Method method, String... paths) {
      resourceManager.registerResource(ctx, () -> {
         Invocations.Builder builder = new Invocations.Builder("test", "testing");
         Arrays.stream(paths).forEach(p -> builder.invocation().method(method).path(p).name(handlerName).handleWith(restRequest -> null));
         return builder.create();
      });
   }

   private void registerHandlerWithAction(String ctx, Method[] methods, String handlerName, String action, String... paths) {
      resourceManager.registerResource(ctx, () -> {
         Invocations.Builder builder = new Invocations.Builder("test", "testing");
         Arrays.stream(paths).forEach(p -> {
            InvocationImpl.Builder invocation = builder.invocation();
            invocation.methods(methods).path(p).name(handlerName).handleWith(restRequest -> null);
            if (action != null) invocation.withAction(action);
         });
         return builder.create();
      });
   }

   private void registerHandler(String ctx, String handlerName, String... paths) {
      registerHandlerWithAction(ctx, new Method[]{GET, HEAD, POST}, handlerName, null, paths);
   }
}
